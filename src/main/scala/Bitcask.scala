package misc
import java.io.{File, RandomAccessFile}
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

class Bitcask(dir:     String,
              timeout: Int = 0) {
  val idx = scala.collection.mutable.Map[Array[Byte], BitcaskEntry]() //FIXME: concurrent access!
  val log = new BitcaskLog(new File(dir))
  log.iterate((e1,e2) => {
    // update idx
    // update stats
  })

  def get(k: Array[Byte]) : Option[Array[Byte]] = {
    idx.get(k) match {
      case Some(e) => log.read(e)
      case None    => None
    }
  }

  def put(k: Array[Byte], v: Array[Byte]) = {
    //val e = create_entry(k, v)
    log.synchronized {
      //val be = log.append(e)
      // update idx
      // update stats
    }
  }

  def delete(k: Array[Byte]) = {
    log.synchronized {
      //log.append(k, "tombstone")
      // update idx
      // update stats
    }
  }

  def list_keys() : List[Array[Byte]] = {
    ???
  }
  /*
  def update_idx(k, be: BitcaskEntry) = {
    idx.get(k) match {
      case Some(_) =>
        // update_stats(oldfile)
        idx.put(k, be)
      case None =>
        idx.put(k, be)
    }
  }
*/
  /*
  def create_entry(k: Array[Byte], v: Array[Byte]) = {
    //crc32 32 bytes
    //tstamp 32bytes // number of seconds since 1970
    //keysize 16bytes
    //valuesize 32bytes
    //key
    //value
    val tstamp = timestamp
    val crc32  = new CRC32()
    crc32.update(timestamp.getBytes)
    crc32.update(k.size.getBytes)
    crc32.update(v.size.getBytes)
    crc32.update(k)
    crc32.update(v)
    val buffer = ByteBuffer.allocate(4 + // crc32
                                     4 + // tstamp
                                     2 + // keysize
                                     4 + // valuesize
                                     k.size +
                                     v.size)
    buffer.putInt((0xFFFFFFFF & crc32.getValue).toInt)
    buffer.putInt(tstamp)
    buffer.putChar((k.size & 0xFFFF).toChar)
    buffer.putInt(v.size)
    buffer.put(k)
    buffer.put(v)
  }
  */
  def timestamp = (System.currentTimeMillis() / 1000).toInt

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

class BitcaskLog(dir: File) {
  var archive = archive_files()
  var next_id = if(archive.size!=0) archive.last+1 else 1
  var active  = open_active()

  // API ---------------------------------------------------------------
  // TODO: index
  def iterate(f: (Array[Byte], BitcaskEntry) => Any) = {
    ???
  }
  def iterate(id: Int, f: (Array[Byte], BitcaskEntry) => Any) = {
    // read files in order, if hintfile exist use that
    ???
  }

  def read(e: BitcaskEntry) = {
    // FIXME: pool
    
    val raf = new RandomAccessFile(filename(e.file_id), "r")
    try {
      raf.seek(e.offset)
      ???
      //raf.read(e.total_size)
    } finally {
      raf.close()
    }
  }

  def switch_log() = {
    active.close()
    // build index
    active = open_active()
  }

  def append(e: Array[Byte]) : BitcaskEntry = {
    // append to latest file
    //if(size >= max_size) {
    //  switch_log()
    //}
    ???
    maybe_switch_log()
  }

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
    var fn = filename(next_id+1)
    next_id+=1
    new RandomAccessFile(fn, "rw")
  }

  private def filename(id: Int) = {
    "bitcask_%08d.data".format(id)
  }
}

