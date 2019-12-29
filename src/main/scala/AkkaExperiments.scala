package misc

import scala.concurrent.Await
import akka.actor._
import akka.util.Timeout
import akka.actor.SupervisorStrategy._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Promise}
import akka.pattern.ask
// import scala.concurrent.ExecutionContext.Implicits.global

class AkkaExperiments {
  val system = ActorSystem("akka")
  val actor1 = system.actorOf(Props(new AkkaTest1()), name = "test1")
  val actor2 = system.actorOf(Props(new AkkaTest2()), name = "test2")

  def test_send() = {
    actor1 ! "test1"
  }

  def test_ask1() = {
    implicit val timeout = Timeout(5.seconds)
    val future = actor1 ? Question
    val result = Await.result(future,
                              timeout.duration).asInstanceOf[String]
    println("result: " + result)
  }

  def test_ask2() = {
    implicit val timeout = Timeout(1.seconds)
    val future = (actor1 ? Question).mapTo[String]
    println(future)
  }
  case object Question

  def test_crash() = {
    actor2 ! "crash"
  }

  def stop1() = {
    system.terminate
  }
  def stop2() = {
    actor1 ! PoisonPill
  }

  class AkkaTest1 extends Actor {
    override def preStart() = { }
    override def postRestart(rsn: Throwable) = { }
    def receive = {
      case Question =>
        sender ! "ok!"
      case "test1" =>
        var res = context.actorSelection("/user/test1")
        println("res = " + res)

        println("context = " + context)
        println("self = " + self)
        println("sender = " + sender)
        println("hello")
      case "question" => sender ! "answer"
      case _       => ???
    }
  }

  class AkkaTest2 extends Actor {
    override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 3,
                                                        withinTimeRange = 5.seconds) {
      case e =>
        println("supervisor got: " + e)
        Thread.sleep(40)
        Restart
    }
    def receive = {
      case x => println("received " + x)
        throw new Exception("crashing")
    }
  }
}
