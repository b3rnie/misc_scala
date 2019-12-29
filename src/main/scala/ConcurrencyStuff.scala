package misc
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

object ConcurrencyStuff {
  def future() = {
    val f = Future {
      throw new Exception("oh no")
      "some result"
    }
    println(f)
    f.onComplete { x =>
      println(x)
    }

    val f2 = Future {
      Thread.sleep(5000L)
      // throw new Exception("a bad result")
    }
    val res = Await.result(f2, Duration(2, SECONDS))
    println(res)
  }
}
