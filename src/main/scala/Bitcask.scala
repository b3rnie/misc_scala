/* Bitcask like storage
 * Björn Jensen-Urstad / 2014
 */
package misc
import java.io.{File,
                RandomAccessFile,
                FileInputStream, BufferedInputStream}
import java.nio.ByteBuffer
import java.util.zip.CRC32
import scala.collection.concurrent.TrieMap
import scala.util.matching.Regex
import com.typesafe.config.ConfigFactory

class Bitcask(dir: File, timeout: Int = 0) extends Logging {
  val config = ConfigFactory.load().getConfig("bitcask")
  val max_size = config.getLong("max_size")
  val compaction_interval = config.getLong("compaction_interval")
  val log = new BitcaskLog(dir, max_size)
  var idx = new BitcaskIdx(dir, log)
  var compactor = new Thread(new Compactor())
  compactor.start()

  def get(k: Array[Byte]) : Option[Array[Byte]] = {
    idx.get(k) match {
      case Some(Bitcask.Entry(pos,tombstone,timestamp))
        if tombstone => None
      case Some(Bitcask.Entry(pos,tombstone,timestamp))
        if Bitcask.is_expired(timeout, timestamp) => None
      case Some(Bitcask.Entry(pos,tombstone,timestamp)) =>
        // read can fail here if compaction just deleted
        // the 'old' file
        var buf = log.read(pos)
        var(hdr,body) = Bitcask.unpack(buf)
        Option(body.value)
      case None =>
        None
    }
  }

  def put(k: Array[Byte], v: Array[Byte]) = {
    require(k.size <= 0xFFFF, "key is too big")
    require(v.size <= 0xFFFFFFFFL, "value is too big")
    val ts = Bitcask.timestamp
    val b  = Bitcask.pack(k,v,ts)
    log.synchronized {
      val pos = log.append(b)
      idx.put(k, Bitcask.Entry(pos       = pos,
                               tombstone = false,
                               timestamp = ts))
    }
  }

  def delete(k: Array[Byte]) : Boolean = {
    idx.get(k) match {
      case Some(Bitcask.Entry(pos,tombstone,timestamp))
        if tombstone => false
      case Some(Bitcask.Entry(pos,tombstone,timestamp))
        if Bitcask.is_expired(timeout, timestamp) => false
      case Some(Bitcask.Entry(pos,tombstone,timestamp)) =>
        val ts = Bitcask.timestamp
        val b  = Bitcask.pack(k,Bitcask.TOMBSTONE.getBytes,ts)
        log.synchronized {
          val pos = log.append(b)
          idx.put(k, Bitcask.Entry(pos       = pos,
                                   tombstone = true,
                                   timestamp = timestamp))
        }
        true
      case None => false
    }
  }

  def compact() = {
    log.synchronized { log.switch_log() }
    /* Rebuilding whole log, kinda retarded but very simple */
    val files = log.archive
    files.foreach(id => {
      Bitcask.iterate(log, id, (k,v,e) => {
        // drop: expired, superseeded, tombstones
        // save: current
        println("compact: " + new String(k))
        val(action,oldpos) =
          idx.get(k) match {
            case Some(Bitcask.Entry(pos,tombstone,timestamp))
              if tombstone =>
              // previous (shadowed) value is already dropped
              // if that exist
              ('drop, pos)
            case Some(Bitcask.Entry(pos,tombstone,timestamp))
              if Bitcask.is_expired(timeout, timestamp) =>
              // expired
              ('drop, pos)
            case Some(Bitcask.Entry(pos,tombstone,timestamp))
              if pos != e.pos =>
              // shadowed by later
              ('drop, pos)
            case Some(Bitcask.Entry(pos,tombstone,timestamp)) =>
              // save
              ('save, pos)
            case None =>
              // should not happen
              throw new Bitcask.BitcaskException("impossible state")
          }
        log.synchronized {
          idx.get(k) match {
            case Some(Bitcask.Entry(pos,tombstone,timestamp))
              if pos == e.pos && action == 'drop =>
              debug("deleting %s from idx".format(new String(k)))
              idx.delete(k)
            case Some(Bitcask.Entry(pos,tombstone,timestamp))
              if pos == e.pos && action == 'save =>
              debug("updating %s in idx".format(new String(k)))
              val b  = Bitcask.pack(k,v,timestamp)
              val newpos = log.append(b)
              idx.put(k, Bitcask.Entry(pos       = newpos,
                                       tombstone = tombstone,
                                       timestamp = timestamp))
            case Some(_) =>
              // idx points to later value
            case None =>
              // someone just deleted it
          }
        }
      })
      log.delete(id)
    })
  }

  def keys() : Iterable[Array[Byte]] = {
    // FIXME: return a proper iterator
    ???
  }

  def stop = {
    compactor.interrupt()
    log.stop
  }

  class Compactor extends Runnable {
    override def run() {
      while (compaction_interval > 0 &&
             !Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(compaction_interval * 1000)
          compact()
        } catch {
          case e: java.lang.InterruptedException =>
        }
      }
    }
  }
}

object Bitcask extends Logging {
  final val TOMBSTONE = "__tombstone__"
  // crc32     - 4 bytes
  // timestamp - 4 bytes
  // keysize   - 2 bytes
  // valuesize - 4 bytes
  final val HEADER_SIZE = 14

  case class Entry (
    pos:       Bitcask.LogEntry,
    tombstone: Boolean,
    timestamp: Long
  )

  case class LogEntry (
    id:         Long,
    offset:     Int,
    total_size: Int
  )

  case class Header (
    crc32:     Long,
    timestamp: Long,
    keysize:   Int,
    valsize:   Int
  )

  case class Body (
    key:   Array[Byte],
    value: Array[Byte]
  )

  class BitcaskException(s: String) extends RuntimeException(s) {}
  class BitcaskLogException(s: String) extends RuntimeException(s) {}
  class BitcaskCRC32Exception(s: String) extends RuntimeException(s) {
    def this() = this(null)
  }
  def pack(k: Array[Byte], v: Array[Byte], ts: Long) : ByteBuffer = {
    // FIXME: max key/val size
    val size = HEADER_SIZE + k.size + v.size
    val crc32  = new CRC32()
    val buffer = ByteBuffer.allocate(size)
    buffer.position(4)
    buffer.putChar(((ts >> 16) & 0xFFFF).toChar)
    buffer.putChar((ts         & 0xFFFF).toChar)
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

  def unpack(b: ByteBuffer) : Tuple2[Bitcask.Header,Bitcask.Body] = {
    // check key
    // check boundaries
    var hdr = unpack_header(b)
    var crc32 = new CRC32()
    var a = b.array()
    crc32.update(a, 4, a.size-4)
    if(crc32.getValue() != hdr.crc32)
      throw new BitcaskCRC32Exception
    var body = unpack_body(b, hdr)
    (hdr,body)
  }

  def unpack_header(b: ByteBuffer) : Bitcask.Header = {
    var crc32val = b.getChar().toLong
    crc32val     = (crc32val << 16) | b.getChar().toLong
    var tstamp   = b.getChar().toLong
    tstamp       = (tstamp << 16) | b.getChar().toLong
    var ksize    = b.getChar().toInt
    var vsize    = b.getChar().toInt
    vsize        = (vsize << 16) | b.getChar().toInt
    Bitcask.Header(crc32     = crc32val,
                   timestamp = tstamp,
                   keysize   = ksize,
                   valsize   = vsize)
  }

  def unpack_body(b: ByteBuffer, hdr: Bitcask.Header) : Bitcask.Body = {
    var k = new Array[Byte](hdr.keysize)
    var v = new Array[Byte](hdr.valsize)
    b.get(k)
    b.get(v)
    Bitcask.Body(key=k, value=v)
  }

  def iterate(log: BitcaskLog,
              id:  Long,
              f:   (Array[Byte], Array[Byte], Bitcask.Entry) => Any) : Unit = {
    log.iterate(id, is => {
      var offset = 0
      def loop: Unit = {
        try_read(Bitcask.HEADER_SIZE, is) match {
          case ReadEof() =>
          case ReadOk(b) =>
            val hdr = Bitcask.unpack_header(ByteBuffer.wrap(b))
            try_read(hdr.keysize+hdr.valsize, is) match {
              case ReadOk(kv) =>
                val body = Bitcask.unpack_body(ByteBuffer.wrap(kv), hdr)
                val pos  = Bitcask.LogEntry(id         = id,
                                            total_size = hdr.keysize+hdr.valsize+Bitcask.HEADER_SIZE,
                                            offset     = offset)
                debug("k = " + new String(body.key))
                debug("v = " + new String(body.value))
                val e = Bitcask.Entry(pos       = pos,
                                      tombstone = Bitcask.is_tombstone(body.value),
                                      timestamp = hdr.timestamp)
                f(body.key,body.value,e)
                offset+=Bitcask.HEADER_SIZE+hdr.keysize+hdr.valsize
                loop
              case ReadEof() =>
                warn("unexpected eof while iterating %d".format(id))
              case ReadShort() =>
                warn("unexpected eof while iterating %d".format(id))
            }
          case ReadShort() =>
            warn("unexpected eof while iterating %d".format(id))
        }
      }
      loop
    })
  }

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

  def is_tombstone(value: Array[Byte]) = {
    ByteBuffer.wrap(value) == ByteBuffer.wrap(TOMBSTONE.getBytes)
  }

  def is_expired(timeout: Int, timestamp: Long) = {
    timeout != 0 && timestamp+timeout >= Bitcask.timestamp
  }

  def timestamp = (System.currentTimeMillis() / 1000).toLong
}

// Simple append only log
class BitcaskLog(dir: File, max_size: Long) extends Logging {
  var archive = archive_files()
  var next_id = if(archive.size!=0) archive.last+1 else 1
  var active  = open_active()
  var active_pos = 0

  info("log starting with dir: %s".format(dir))
  archive.foreach(f => info("archive file: %s".format(f)))

  // API ---------------------------------------------------------------
  def iterate(id: Long, f: BufferedInputStream => Any) : Unit = {
    val is = new BufferedInputStream(
      new FileInputStream(new File(dir, filename(id))))
    try { f(is) } finally { is.close }
  }

  def read(e: Bitcask.LogEntry) : ByteBuffer = {
    // FIXME: pool
    val raf = new RandomAccessFile(new File(dir, filename(e.id)), "r")
    try {
      var buff = ByteBuffer.allocate(e.total_size)
      var chan = raf.getChannel()
      chan.position(e.offset)
      if(raf.getChannel().read(buff)!=e.total_size)
        throw new Bitcask.BitcaskLogException("unable to read entry")
      buff.flip()
      buff
    } finally {
      raf.close()
    }
  }

  def switch_log() = {
    active.close()
    archive = archive :+ next_id-1
    active = open_active()
  }

  def append(e: ByteBuffer) : Bitcask.LogEntry = {
    var pos = active.getFilePointer().toInt
    if(pos >= max_size) {
      switch_log()
      pos = 0
    }
    active.getChannel.write(e)
    Bitcask.LogEntry(id         = next_id -1,
                     offset     = pos,
                     total_size = (active.getFilePointer()-pos).toInt)
  }

  def delete(id: Long) = {
    archive = archive.filter(_ != id)
    new File(dir, filename(id)).delete()
  }

  def stop = {
    active.close()
  }

  // Internal ----------------------------------------------------------
  private def archive_files() = {
    val re = """^bitcask_(\d+){19}\.data$""".r
    val res = dir.list().flatMap(e => {
      re.findFirstMatchIn(e.toString) match {
        case Some(m) => Seq(m.group(1).toInt)
        case None    => Nil
      }
    }).sortWith(_<_)
    res
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
  val idx = new TrieMap[ByteBuffer,Bitcask.Entry]

  // FIXME: dont traverse everything on startup
  log.archive.foreach(id => {
    debug("traversing (%d)".format(id))
    Bitcask.iterate(log, id, (k,v,e) => put(k,e))
  })
  // API ---------------------------------------------------------------
  def get(k: Array[Byte]) : Option[Bitcask.Entry] = {
    idx.get(ByteBuffer.wrap(k))
  }

  def put(k: Array[Byte], e: Bitcask.Entry) = {
    idx.put(ByteBuffer.wrap(k), e) match {
      case Some(old) =>
      case None =>
    }
  }

  def delete(k: Array[Byte]) = {
    idx.remove(ByteBuffer.wrap(k))
  }
}
