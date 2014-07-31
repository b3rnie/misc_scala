import misc._
import scala.util.{Random}
import org.scalatest._

class BinHeapTest extends FlatSpec {
  "A MaxBinHeap" should "return the entries in correct order" in {
    val maxheap = new MaxBinHeap[Int,Int]()
    val minheap = new MinBinHeap[Int,Int]()
    val rand    = new Random()
    val list    = (1 to 1000).map { _ => rand.nextInt() }
    list.foreach(n => {
      maxheap.insert(n,n)
      minheap.insert(n,n)
    })
    assert(minheap.size() === 1000)
    assert(maxheap.size() === 1000)
    list.sortWith(_ > _).foreach(n => {
      assert(maxheap.head()._1 === n)
    })
    list.sortWith(_ < _).foreach(n => {
      assert(minheap.head()._1 === n)
    })
    assert(minheap.size() === 0)
    assert(maxheap.size() === 0)
  }
}
