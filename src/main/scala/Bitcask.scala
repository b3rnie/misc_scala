package misc
import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.util.zip.CRC32
import scala.util.matching.Regex



case class BitcaskEntry(
  file_id:    Int,
  offset:     Long,
  total_size: Long
)

case class FStats(
  file_id:       Int,
  live_keys:     Long,
  live_bytes:    Long,
  total_keys:    Long,
  total_bytes:   Long,
  oldest_tstamp: Int,
  newest_tstamp: Int
)

class Bitcask(dir: File, timeout: Int = 0) {
  val idx = scala.collection.mutable.Map[ByteBuffer,
                                         BitcaskEntry]() //FIXME: concurrent access!
  val log = new BitcaskLog(dir)
  log.iterate((e1,e2) => {
    // update idx
    // update stats
  })

  def get(k: Array[Byte]) : Option[Array[Byte]] = {
    var res = idx.synchronized { idx.get(ByteBuffer.wrap(k)) }
    res match {
      case Some(e) =>
        log.read(e) match {
          case Some(buff) =>
            var(key, value) = unpack(buff)
            Option(value)

          case None =>
            // log, should not happen
            None
        }
      case None =>
        None
    }
  }

  def put(k: Array[Byte], v: Array[Byte]) = {
    val b = pack(k,v)
    log.synchronized {
      val be = log.append(b)
      idx.synchronized { update_idx(k, be) }
    }
    println("size = " + idx.size)
  }

  def delete(k: Array[Byte]) = {
    log.synchronized {
      //log.append(k, "tombstone")
      // update_idx
      // update stats
    }
  }

  def list_keys() : List[Array[Byte]] = {
    ???
  }

  def update_idx(k: Array[Byte], be: BitcaskEntry) = {
    println("k = " + new String(k))
    idx.put(ByteBuffer.wrap(k), be) match {
      case Some(_) =>
        println("dec stats")
        // inc_stats()
        // dec_stats()
      case None =>
        // inc_stats()
    }
  }

  def timestamp = (System.currentTimeMillis() / 1000).toLong

  def merge = {
    // figure out which files need merging (stats/etc)
    // iterate file, for each entry decide if it should
    // be kept (future deletes / expiry)
    log.synchronized {
      log.switch_log // new 'empty'
      log.switch_log // new 'active'
    }
  }

  def pack(k: Array[Byte], v: Array[Byte]) : ByteBuffer = {
    // FIXME: max key/val size
    val size   = 4 + // crc32
                 4 + // timestamp // number of seconds since 1970
                 2 + // keysize
                 4 + // valuesize
                 k.size +
                 v.size
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
    // check crc32
    // check boundaries
    var crc32val = b.getChar().toLong
    crc32val = (crc32val << 16) | b.getChar().toLong
    var tstamp = b.getChar().toLong
    tstamp = (tstamp << 16) | b.getChar().toLong
    var ksize = b.getChar().toLong
    var vsize = b.getChar().toLong
    vsize = (vsize << 16) | b.getChar().toLong
    println(ksize)
    println(vsize)
    var kk = new Array[Byte](ksize.toInt)
    var vv = new Array[Byte](vsize.toInt)
    b.get(kk)
    b.get(vv)
    Tuple2(kk, vv)
  }
}

class BitcaskLog(dir: File) {
  var archive = archive_files()
  var next_id = if(archive.size!=0) archive.last+1 else 1
  var active  = open_active()

  // API ---------------------------------------------------------------
  // TODO: index
  def iterate(f: (Array[Byte], BitcaskEntry) => Any) = {
    
  }
  def iterate(id: Int, f: (Array[Byte], BitcaskEntry) => Any) = {
    // read files in order, if hintfile exist use that
  }

  def read(e: BitcaskEntry) : Option[ByteBuffer] = {
    // FIXME: pool
    val raf = new RandomAccessFile(
      new File(dir, filename(e.file_id)), "r")
    try {
      var buff = ByteBuffer.allocate(e.total_size.toInt)
      var chan = raf.getChannel()
      chan.position(e.offset)
      if(raf.getChannel().read(buff)!=e.total_size) {
        // warn
        None
      } else{
        buff.flip()
        Option(buff)
      }
    } finally {
      raf.close()
    }
  }

  def switch_log() = {
    active.close()
    // build index
    active = open_active()
  }

  def append(e: ByteBuffer) : BitcaskEntry = {
    var pos = active.getFilePointer()
    active.getChannel.write(e)
    BitcaskEntry(file_id    = next_id -1,
                 offset     = pos,
                 total_size = active.getFilePointer()-pos)
  }
  //def append(e: Array[Byte]) : BitcaskEntry = {
    // append to latest file
    //if(size >= max_size) {
    //  switch_log()
    //}
  //  ???
  //  maybe_switch_log()
  //}

  // Internal ----------------------------------------------------------
  private def clean_tmp_files() = {
    ???
  }

  private def archive_files() = {
    val re = """^bitcask_(\d+){8}\.data$""".r
    dir.listFiles().flatMap(e => {
      re.findFirstMatchIn(e.toString) match {
        case Some(m) => Seq(m.group(1).toInt)
        case None    => Nil
      }
    }).sortWith(_<_)
  }

  def build_index(id: Int) = {
    ???
  }

  private def maybe_switch_log() = {
    ???
  }

  private def open_active() = {
    var fn = filename(next_id)
    next_id+=1
    new RandomAccessFile(new File(dir, fn), "rw")
  }

  private def filename(id: Int) = {
    "bitcask_%08d.data".format(id)
  }
}