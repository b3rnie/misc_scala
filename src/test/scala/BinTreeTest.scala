import misc._
import org.scalatest._
import scala.concurrent._
import scala.concurrent.duration._

class BinTreeTest extends FlatSpec {
  "BinTree" should "do its thing" in {
    val t0 = new BinTree[Int,Int]()
    val r  = randomNumbers()
    r.foreach(n => {
      assert(t0.lookup(n) === None)
      t0.insert(n,n)
    })
    r.foreach(n => {
      assert(t0.lookup(n).get === n)
      assert(t0.remove(n).get === n)
      assert(t0.lookup(n) === None)
    })
    t0.print()
  }

  def randomNumbers() = {
    scala.util.Random.shuffle(1.to(100))
  }
}
