import misc._
import org.scalatest._
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

class ConcurrencyTest extends FlatSpec {
  "test" should "work" in {
    // ConcurrencyStuff.future()
    val f = Future { 1 }
    val g = Future { 2 }
    val z = Future {
      f.flatMap { (x: Int) => g.map { (y: Int) => x + y } }
    }.flatMap(gg => gg)

    z.onComplete(x => println("COMPLETE = " + x))
    Thread.sleep(100)
  }
}
