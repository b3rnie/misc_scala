import misc._
import java.io.{File}
import scala.util.{Random}
import org.scalatest._

class BitcaskTest extends FlatSpec {
  "Bitcask" should "support put/get/delete" in {
    var bc = new Bitcask(init_dir)
    bc.put("foo".getBytes(), "123".getBytes())
    bc.put("bar".getBytes(), "567".getBytes())
    bc.put("bar".getBytes(), "912".getBytes())
    assert(bc.get("foo".getBytes()).get === "123".getBytes)
    assert(bc.get("bar".getBytes()).get === "912".getBytes)
    bc.delete("foo".getBytes)
    assert(bc.get("foo".getBytes) === None)
  }

  "Bitcask" should "start and stop correctly" in {
    var dir = init_dir
    var bc1 = new Bitcask(dir)
    bc1.put("foo".getBytes(), "123".getBytes())
    bc1.put("bar".getBytes(), "456".getBytes())
    bc1.delete("bar".getBytes())
    bc1.stop
    var bc2 = new Bitcask(dir)
    assert(bc2.get("foo".getBytes()).get === "123".getBytes())
    assert(bc2.get("bar".getBytes())     === None)
  }

  "Bitcask" should "compact" in {
    var dir = init_dir
    var bc  = new Bitcask(dir)
    bc.put("foo".getBytes(), "123".getBytes())
    bc.put("bar".getBytes(), "456".getBytes())
    bc.put("baz".getBytes(), "789".getBytes())
    bc.delete("foo".getBytes())
    bc.put("bar".getBytes(), "012".getBytes())
    bc.compact()
    assert(bc.get("foo".getBytes())     === None)
    assert(bc.get("bar".getBytes()).get === "012".getBytes())
    assert(bc.get("baz".getBytes()).get === "789".getBytes())
  }

  def init_dir = {
    var dir = new File("bitcask_test")
    if(dir.exists()) {
      dir.listFiles().foreach(f => f.delete())
    } else {
      dir.mkdirs()
    }
    dir
  }
}
