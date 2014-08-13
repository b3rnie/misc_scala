/* Bitcask like storage
 * Björn Jensen-Urstad / 2014
 */
package misc
import java.io.{File,
                RandomAccessFile,
                FileInputStream, BufferedInputStream}
import java.nio.ByteBuffer
import java.util.zip.CRC32
import scala.util.matching.Regex

case class BitcaskLogEntry (
    id:         Long,
    offset:     Int,
    total_size: Int
)

class BitcaskException(s: String) extends RuntimeException(s) {}
class BitcaskLogException(s: String) extends RuntimeException(s) {}

class Bitcask(dir: File, timeout: Int = 0) extends Logging {
  // FIXME: move to actors
  val log   = new BitcaskLog(dir)
  var idx   = new BitcaskIdx(dir, log)

  def get(k: Array[Byte]) : Option[Array[Byte]] = {
    idx.synchronized { idx.get(k) } match {
      case Some(e) =>
        var buff = log.read(e)
        var(key, value) = BitcaskData.unpack(buff)
        Option(value)
      case None =>
        None
    }
  }

  def put(k: Array[Byte], v: Array[Byte]) = {
    val b = BitcaskData.pack(k,v)
    log.synchronized {
      val be = log.append(b)
      idx.synchronized { idx.put(k, be) }
    }
  }

  def delete(k: Array[Byte]) = {
    idx.synchronized { idx.get(k) }.foreach(_ => {
      val b = BitcaskData.pack(k, "__tombstone__".getBytes)
      log.synchronized {
        val be = log.append(b)
        idx.synchronized { idx.remove(k,be) }
      }
    })
  }

  def list_keys() : List[Array[Byte]] = {
    ???
  }

  def stop = {

  }


  def merge = {
    // figure out which files need merging (stats/etc)
    // iterate file, for each entry decide if it should
    // be kept (future deletes / expiry)
    log.synchronized {
      log.switch_log // new 'empty'
      log.switch_log // new 'active'
    }
  }
}

object BitcaskData {
  // crc32     - 4 bytes
  // timestamp - 4 bytes
  // keysize   - 2 bytes
  // valuesize - 4 bytes
  var HEADER_SIZE = 14

  def pack(k: Array[Byte], v: Array[Byte]) : ByteBuffer = {
    // FIXME: max key/val size
    val size = HEADER_SIZE + k.size + v.size
    val crc32  = new CRC32()
    val buffer = ByteBuffer.allocate(size)
    buffer.position(4)
    val tstamp = timestamp
    buffer.putChar(((tstamp >> 16) & 0xFFFF).toChar)
    buffer.putChar((tstamp         & 0xFFFF).toChar)
    buffer.putChar(k.size.toChar)
    buffer.putInt(v.size)
    buffer.put(k)
    buffer.put(v)
    buffer.position(0)
    crc32.update(buffer.array(), 4, size-4)
    val crc32val = crc32.getValue()
    buffer.putChar(((crc32val >> 16) & 0xFFFF).toChar)
    buffer.putChar((crc32val         & 0xFFFF).toChar)
    buffer.rewind()
    buffer
  }

  def unpack(b: ByteBuffer) : Tuple2[Array[Byte], Array[Byte]] = {
    // check key
    // check boundaries
    var(crc32val, tstamp, ksize, vsize) = unpack_header(b)
    var crc32 = new CRC32()
    var a = b.array()
    crc32.update(a, 4, a.size-4)
    println("size = " + a.size)
    println("current = " + (crc32.getValue() & 0xFFFFFFFF))
    println("expected = " + crc32val)
    if(crc32.getValue() != crc32val)
      throw new CRC32Exception
    var(k,v) = unpack_body(b,ksize,vsize)
    (k,v)
  }

  def unpack_header(b: ByteBuffer) = {
    var crc32val = b.getChar().toLong
    crc32val     = (crc32val << 16) | b.getChar().toLong
    var tstamp   = b.getChar().toLong
    tstamp       = (tstamp << 16) | b.getChar().toLong
    var ksize    = b.getChar().toInt
    var vsize    = b.getChar().toInt
    vsize        = (vsize << 16) | b.getChar().toInt
    (crc32val, tstamp, ksize, vsize)
  }

  def unpack_body(b: ByteBuffer, ksize: Int, vsize: Int) = {
    var k = new Array[Byte](ksize)
    var v = new Array[Byte](vsize)
    b.get(k)
    b.get(v)
    (k,v)
  }

  def timestamp = (System.currentTimeMillis() / 1000).toLong

  class CRC32Exception(s: String) extends RuntimeException(s) {
    def this() = this(null)
  }
}

class BitcaskLog(dir: File) extends Logging {
  var archive = archive_files()
  var next_id = if(archive.size!=0) archive.last+1 else 1
  var active  = open_active()

  // API ---------------------------------------------------------------
  def iterate(f: (Long, BufferedInputStream) => Any) : Unit = {
    archive_files.foreach(id => iterate(id, f))
    iterate(next_id-1, f)
  }

  def iterate(id: Long, f: (Long, BufferedInputStream) => Any) : Unit = {
    val is = new BufferedInputStream(
      new FileInputStream(new File(dir, filename(id))))
    try { f(id, is) } finally { is.close }
  }

  def read(e: BitcaskLogEntry) : ByteBuffer = {
    // FIXME: pool
    val raf = new RandomAccessFile(new File(dir, filename(e.id)), "r")
    try {
      var buff = ByteBuffer.allocate(e.total_size)
      var chan = raf.getChannel()
      chan.position(e.offset)
      if(raf.getChannel().read(buff)!=e.total_size)
        throw new BitcaskLogException("unable to read entry")
      buff.flip()
      buff
    } finally {
      raf.close()
    }
  }

  def switch_log() = {
    active.close()
    active = open_active()
  }

  def append(e: ByteBuffer) : BitcaskLogEntry = {
    // if(size >= max_size) switch_log
    var pos = active.getFilePointer().toInt
    active.getChannel.write(e)
    BitcaskLogEntry(id         = next_id -1,
                    offset     = pos,
                    total_size = (active.getFilePointer()-pos).toInt)
  }

  def delete(id: Long) = {
    new File(dir, filename(id)).delete()
  }

  // Internal ----------------------------------------------------------
  private def archive_files() = {
    val re = """^bitcask_(\d+){19}\.data$""".r
    dir.listFiles().flatMap(e => {
      re.findFirstMatchIn(e.toString) match {
        case Some(m) => Seq(m.group(1).toInt)
        case None    => Nil
      }
    }).sortWith(_<_)
  }

  private def open_active() = {
    var fn = filename(next_id)
    next_id+=1
    new RandomAccessFile(new File(dir, fn), "rw")
  }

  private def filename(id: Long) = {
    "bitcask_%019d.data".format(id)
  }
}

class BitcaskIdx(dir: File, log: BitcaskLog) extends Logging {
  val idx   = scala.collection.mutable.Map[ByteBuffer,BitcaskLogEntry]()
  val stats = scala.collection.mutable.Map[Long, LogStats]()
  // TODO: store this information on disk so we dont have to
  // iterate through all logfiles.
  log.iterate((id, is) => {
    var offset = 0
    def f: Unit = {
      try_read(BitcaskData.HEADER_SIZE, is) match {
        case ReadEof() =>
        case ReadOk(b) =>
          val(crc32, tstamp, ksize, vsize) = BitcaskData.unpack_header(ByteBuffer.wrap(b))
          try_read(ksize+vsize, is) match {
            case ReadOk(kv) =>
              val(k,v) = BitcaskData.unpack_body(ByteBuffer.wrap(kv), ksize,vsize)
              val be = BitcaskLogEntry(id         = id,
                                       total_size = ksize+vsize+BitcaskData.HEADER_SIZE,
                                       offset     = offset)
              debug("k = " + new String(k))
              debug("v = " + new String(v))
              if(ByteBuffer.wrap(v) == ByteBuffer.wrap("__tombstone__".getBytes))
                remove(k, be)
              else
                put(k, be)
              offset+=BitcaskData.HEADER_SIZE+ksize+vsize
            case ReadEof() =>
              warn("unexpected eof while iterating %d".format(id))
            case ReadShort() =>
              warn("unexpected eof while iterating %d".format(id))
          }
          f
        case ReadShort() =>
          warn("unexpected eof while iterating %d".format(id))
      }
    }
    f
  })

  sealed abstract class Read
  case class ReadEof()              extends Read
  case class ReadOk(b: Array[Byte]) extends Read
  case class ReadShort()            extends Read

  def try_read(n: Int, is: BufferedInputStream) : Read = {
    var buf  = new Array[Byte](n)
    var left = n
    var off  = 0
    var r = 0
    do {
      // omg: flashback from the nineties and programming c.
      r = is.read(buf, off, left)
      if(r>0) {
        left-=r
        off+=r
      }
    } while(r>0 && left>0)
    if(r<0 && off==0) return ReadEof()
    else if(r<=0)     return ReadShort()
    else              return ReadOk(buf)
  }

  def get(k: Array[Byte]) = {
    idx.get(ByteBuffer.wrap(k))
  }

  def put(k: Array[Byte], v: BitcaskLogEntry) = {
    inc(v)
    idx.put(ByteBuffer.wrap(k),v).foreach(old => dec(old))
  }

  def remove(k: Array[Byte], be: BitcaskLogEntry) = {
    idx.remove(ByteBuffer.wrap(k)) match {
      case Some(old) =>
      case None      =>
    }
  }

  case class LogStats(
    live_keys:     Long
    /*
    live_bytes:    Long,
    total_keys:    Long,
    total_bytes:   Long,
    oldest_tstamp: Int,
    newest_tstamp: Int
    */
  )

  def inc(be: BitcaskLogEntry) = {
    stats.get(be.id) match {
      case Some(s) => stats.put(be.id, s.copy(live_keys = s.live_keys+1))
      case None    => stats.put(be.id, LogStats(live_keys=1))
    }
  }

  def dec(be: BitcaskLogEntry) = {
    stats.get(be.id) match {
      case Some(s) => stats.put(be.id, s.copy(live_keys = Math.max(s.live_keys-1, 0)))
      case None    => stats.put(be.id, LogStats(live_keys=0))
    }
  }
}
