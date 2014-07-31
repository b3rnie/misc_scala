import misc._
import scala.util.{Random}
import org.scalatest._

class BinHeapTest extends FlatSpec {
  "A BinHeap" should "return the entries in correct order" in {
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
    assert(maxheap.size() === 0)
    assert(minheap.size() === 0)
  }

  "A BinHeap" should "initialize correctly" in {
    val l1 = List(15,16,25,12,14,9,4,5,6,23,27,20,7,8,11)
    val l2 = l1.map(e => Tuple2(e,e))
    val heap = new MinBinHeap[Int,Int](l2.iterator)
    while(heap.size() != 0) {
      println("head = " + heap.head())
    }
  }
}
