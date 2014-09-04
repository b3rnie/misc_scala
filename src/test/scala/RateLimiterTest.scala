import misc._
import org.scalatest._
import scala.concurrent._
import scala.concurrent.duration._

class RateLimiterTest extends FlatSpec {
  "ratelimiter" should "do its thing" in {
    val r  = new RateLimiter(1, 1)
    val f1 = r.execute(() => { 1 })
    val f2 = r.execute(() => { 2 })
    val f3 = r.execute(() => { 3 })

    import ExecutionContext.Implicits.global
    f1.onSuccess { case msg => println("future1 = " + msg)}
    f2.onSuccess { case msg => println("future2 = " + msg)}
    f3.onSuccess { case msg => println("future3 = " + msg)}

    Thread.sleep(5000)
    r.stop()
  }
}
