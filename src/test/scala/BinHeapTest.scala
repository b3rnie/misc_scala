import misc._
import scala.util.{Random}
import org.scalatest._

class BinHeapTest extends FlatSpec {
  "A BinHeap" should "return the entries in correct order" in {
    val heap = new BinHeap[Int]()
    val rand = new Random()
    val list = (1 to 1000).map { _ => rand.nextInt() }
    list.foreach(n => heap.insert(n))
    assert(heap.size() === 1000)
    list.sortWith(_ > _).foreach(n => {
      println(n)
      assert(heap.peek() === n)
      assert(heap.head() === n)
    })
    assert(heap.size() === 0)
  }
}
