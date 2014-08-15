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

case class BitcaskEntry (
  pos:       BitcaskLogEntry,
  tombstone: Boolean,
  timestamp: Long
)

case class BitcaskLogEntry (
  id:         Long,
  offset:     Int,
  total_size: Int
)

case class BitcaskHeader (
  crc32:     Long,
  timestamp: Long,
  keysize:   Int,
  valsize:   Int
)

case class BitcaskBody (
  key:   Array[Byte],
  value: Array[Byte]
)

case class LogStats(
  live_keys:     Long = 0,
  live_bytes:    Long = 0,
  total_keys:    Long = 0,
  total_bytes:   Long = 0,
  oldest_tstamp: Long = Long.MaxValue,
  newest_tstamp: Long = Long.MinValue
)

class BitcaskException(s: String) extends RuntimeException(s) {}
class BitcaskLogException(s: String) extends RuntimeException(s) {}

class Bitcask(dir: File, timeout: Int = 0) extends Logging {
  // FIXME: move to actors
  val log = new BitcaskLog(dir)
  var idx = new BitcaskIdx(dir, log)

  def get(k: Array[Byte]) : Option[Array[Byte]] = {
    idx.get(k) match {
      case Some(BitcaskEntry(pos,tombstone,timestamp))
        if tombstone => None
      case Some(e @ BitcaskEntry(pos,tombstone,timestamp))
        if is_expired(timestamp) =>
        idx.dec(e)
        None
      case Some(BitcaskEntry(pos,tombstone,timestamp)) =>
        var buf = log.read(pos)
        var(hdr,body) = Bitcask.unpack(buf)
        Option(body.value)
      case None =>
        None
    }
  }

  def put(k: Array[Byte], v: Array[Byte]) = {
    val ts = Bitcask.timestamp
    val b  = Bitcask.pack(k,v,ts)
    log.synchronized {
      val pos = log.append(b)
      idx.put(k, BitcaskEntry(pos       = pos,
                              tombstone = false,
                              timestamp = ts))
    }
  }

  def delete(k: Array[Byte]) : Boolean = {
    idx.get(k) match {
      case Some(BitcaskEntry(pos,tombstone,timestamp))
        if tombstone =>
        false
      case Some(e @ BitcaskEntry(pos,tombstone,timestamp))
        if is_expired(timestamp) =>
        idx.dec(e)
        false
      case Some(BitcaskEntry(pos,tombstone,timestamp)) =>
        val ts = Bitcask.timestamp
        val b  = Bitcask.pack(k,Bitcask.TOMBSTONE.getBytes,ts)
        log.synchronized {
          val pos = log.append(b)
          idx.put(k, BitcaskEntry(pos       = pos,
                                  tombstone = true,
                                  timestamp = timestamp))
        }
        true
      case None => false
    }
  }

  def compact() = {
    // This could be done more clever by using our statistics
    // either way, rebuild everything for now
    val files = log.archive
    files.foreach(id => {
      Bitcask.iterate(log, id, (k,v,e) => {
        // drop: expired, superseeded, tombstones
        // save: current
        idx.get(k) match {
          case Some(BitcaskEntry(pos,tombstone,timestamp))
            if tombstone =>
          // previous (shadowed) value is dropped
          case Some(BitcaskEntry(pos,tombstone,timestamp))
            if is_expired(timestamp) =>
          // expired
          case Some(BitcaskEntry(pos,tombstone,timestamp))
            if pos != e.pos =>
          // shadowed by later
          case Some(BitcaskEntry(pos,tombstone,timestamp)) =>
            // save
        }
        // something might have been appended to the log,
        // dont blindly append!
      })
      // log.delete(id)
    })
  }

  def list_keys() : List[Array[Byte]] = {
    ???
  }

  def stop = {
    log.stop
  }

  def is_expired(timestamp: Long) = {
    timeout != 0 && timestamp+timeout >= Bitcask.timestamp
  }
}

object Bitcask extends Logging {
  final val TOMBSTONE = "__tombstone__"

  // crc32     - 4 bytes
  // timestamp - 4 bytes
  // keysize   - 2 bytes
  // valuesize - 4 bytes
  var HEADER_SIZE = 14

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

  def unpack(b: ByteBuffer) : Tuple2[BitcaskHeader,BitcaskBody] = {
    // check key
    // check boundaries
    var hdr = unpack_header(b)
    var crc32 = new CRC32()
    var a = b.array()
    crc32.update(a, 4, a.size-4)
    println("size = " + a.size)
    println("current = " + (crc32.getValue() & 0xFFFFFFFF))
    println("expected = " + hdr.crc32)
    if(crc32.getValue() != hdr.crc32)
      throw new CRC32Exception
    var body = unpack_body(b, hdr)
    (hdr,body)
  }

  def unpack_header(b: ByteBuffer) : BitcaskHeader = {
    var crc32val = b.getChar().toLong
    crc32val     = (crc32val << 16) | b.getChar().toLong
    var tstamp   = b.getChar().toLong
    tstamp       = (tstamp << 16) | b.getChar().toLong
    var ksize    = b.getChar().toInt
    var vsize    = b.getChar().toInt
    vsize        = (vsize << 16) | b.getChar().toInt
    BitcaskHeader(crc32     = crc32val,
                  timestamp = tstamp,
                  keysize   = ksize,
                  valsize   = vsize)
  }

  def unpack_body(b: ByteBuffer, hdr: BitcaskHeader) : BitcaskBody = {
    var k = new Array[Byte](hdr.keysize)
    var v = new Array[Byte](hdr.valsize)
    b.get(k)
    b.get(v)
    BitcaskBody(key=k, value=v)
  }

  def iterate(log: BitcaskLog,
              id:  Long,
              f:   (Array[Byte], Array[Byte], BitcaskEntry) => Any) : Unit = {
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
                val pos  = BitcaskLogEntry(id         = id,
                                           total_size = hdr.keysize+hdr.valsize+Bitcask.HEADER_SIZE,
                                           offset     = offset)
                debug("k = " + new String(body.key))
                debug("v = " + new String(body.value))
                val e = BitcaskEntry(pos       = pos,
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

  def timestamp = (System.currentTimeMillis() / 1000).toLong

  class CRC32Exception(s: String) extends RuntimeException(s) {
    def this() = this(null)
  }
}

// Simple append only log
class BitcaskLog(dir: File) extends Logging {
  var archive = archive_files()
  var next_id = if(archive.size!=0) archive.last+1 else 1
  var active  = open_active()

  info("log starting with dir: %s".format(dir))
  archive.foreach(f => info("archive file: %s".format(f)))

  // API ---------------------------------------------------------------
  def iterate(id: Long, f: BufferedInputStream => Any) : Unit = {
    val is = new BufferedInputStream(
      new FileInputStream(new File(dir, filename(id))))
    try { f(is) } finally { is.close }
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
  val idx   = new TrieMap[ByteBuffer,BitcaskEntry]
  val stats = scala.collection.mutable.Map[Long, LogStats]()

  // FIXME: dont traverse everything on startup
  log.archive.foreach(id => {
    debug("traversing (%d)".format(id))
    Bitcask.iterate(log, id, (k,v,e) => put(k,e))
  })
  // API ---------------------------------------------------------------
  def get(k: Array[Byte]) : Option[BitcaskEntry] = {
    idx.get(ByteBuffer.wrap(k))
  }

  def put(k: Array[Byte], e: BitcaskEntry) = {
    idx.put(ByteBuffer.wrap(k), e) match {
      case Some(old) if e.tombstone => dec(old); dec(e)
      case Some(old)                => dec(old); inc(e)
      case None if e.tombstone      =>
        // tombstones aren't supposed to be appended but we can reach
        // this point anyway due to race-conditions
        dec(e)
      case None                     => inc(e)
    }
  }

  def inc(e: BitcaskEntry) = do_update(e, 'inc)
  def dec(e: BitcaskEntry) = do_update(e, 'dec)

  def do_update(e: BitcaskEntry, Op: Symbol) = {
    val s    = stats.getOrElse(e.pos.id, LogStats())
    val size = e.pos.total_size-Bitcask.HEADER_SIZE
    stats.put(e.pos.id,
              LogStats(live_keys     = if(Op == 'inc) s.live_keys+1
                                       else Math.max(s.live_keys-1, 0),
                       live_bytes    = if(Op == 'inc) s.live_bytes+size
                                       else Math.max(s.live_bytes-size, 0),
                       total_keys    = s.total_keys+1,
                       total_bytes   = s.total_bytes+size,
                       oldest_tstamp = Math.min(s.oldest_tstamp, e.timestamp),
                       newest_tstamp = Math.max(s.newest_tstamp, e.timestamp)))
  }
}
