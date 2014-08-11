import misc._
import java.io.{File}
import scala.util.{Random}
import org.scalatest._

class BitcaskTest extends FlatSpec {
  "Bitcask" should "initialize correctly" in {
    var f = new File("bitcask_test")
    init_dir(f)
  }

  def init_dir(dir: File) = {
    if(dir.exists()) {
      dir.listFiles().foreach(f => f.delete())
    } else {
      dir.mkdirs()
    }
  }
}
