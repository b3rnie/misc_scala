import misc._
import org.scalatest._

class PQTest extends FlatSpec {
  "PQ" should "do its thing" in {
    val ordering = new Ordering[(Int,Int)] {
      def compare(a: (Int,Int), b: (Int,Int)) = b._1.compareTo(a._1)
    }
    val q = new scala.collection.mutable.PriorityQueue[(Int,Int)]()(ordering)

    q.enqueue(1 -> 10)
    q.enqueue(6 -> 3)
    q.enqueue(0 -> 991)
    q.enqueue(3 -> 5)
    println(q.dequeue)
    println(q.dequeue)
    println(q.dequeue)
    sealed trait Person[T]
    case class Msg1[T](y: T) extends Person[T]
    case class Msg2[T](x: T) extends Person[T]

    val m1 = Msg1[Int](10)
    println(m1)
  }
  "TreeSet" should "work" in { 
    val q = new scala.collection.mutable.TreeSet[(Int,Int)]()(
      new Ordering[(Int,Int)] {
        def compare(a: (Int,Int), b: (Int,Int)) =
          b._1.compareTo(a._1)
      })
    val n = randomNumbers(50)
    println("Inserting..")
    n.foreach(n => q.add(n -> n))
    println("Q SIZE = " + q.size)

    n.foreach(n => {
      val(a,b) = q.head
      q.remove(a -> b)
      println("TreeSet = " + a)
    })
  }

  "TreeMap" should "work" in {
    var q = new scala.collection.immutable.TreeMap[Int,String]()(
      new Ordering[Int] {
        def compare(a: Int, b: Int) = a.compareTo(b)
      })
    
    val n = randomNumbers(5000000)
    n.foreach(n => q += (n -> n.toString))
    println("Q SIZE = " + q.size)

    n.foreach(n => {
      val(a,b) = q.head
      q -= a
      println(a)
    })
  }

  def randomNumbers(n: Int) = {
    scala.util.Random.shuffle(1.to(n))
  }
}
