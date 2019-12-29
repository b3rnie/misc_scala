import misc._
import org.scalatest._

class ActorTest extends FlatSpec {
  "akka" should "do its thing" in {
    val a1 = new AkkaExperiments
    val a2 = new AkkaExperiments
    a2.test_send()
    a2.test_ask1()
    a2.test_ask2()
    a1.test_crash()
    Thread.sleep(400)
    a1.stop1()
    a2.stop2()
  }
}
