import misc._
import java.io.{File}
import scala.util.{Random}
import org.scalatest._

class BitcaskTest extends FlatSpec {
  "Bitcask" should "initialize correctly" in {
    var f = new File("bitcask_test")
    init_dir(f)
    var bc = new Bitcask(f)
    bc.put("foo".getBytes(), "123".getBytes())
    bc.put("bar".getBytes(), "567".getBytes())
    bc.put("bar".getBytes(), "912".getBytes())
    assert(bc.get("foo".getBytes()).get === "123".getBytes)
    assert(bc.get("bar".getBytes()).get === "912".getBytes)
  }

  def init_dir(dir: File) = {
    if(dir.exists()) {
      dir.listFiles().foreach(f => f.delete())
    } else {
      dir.mkdirs()
    }
  }
}
