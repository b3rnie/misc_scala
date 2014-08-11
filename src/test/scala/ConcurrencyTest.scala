import misc._
import org.scalatest._

class ConcurrencyTest extends FlatSpec {
  "test" should "work" in {
    ConcurrencyStuff.future()
  }
}
