package misc

import akka.actor._
import akka.pattern.{ask,pipe}
import akka.util.Timeout
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Success,Failure}
import scala.collection.mutable.{Queue,MutableList}

class RateLimiter(n: Int, s: Int) {
  val system = ActorSystem("RateLimiter")
  val actor  = system.actorOf(Props(new Master()), name = "master")

  case object Stop
  case object Tick
  case class Work(work: () => Any)

  def execute(work: () => Any) : Future[Any] = {
    implicit val timeout = Timeout(5.seconds)
    actor ? Work(work)
  }

  def stop() = {
    actor ! Stop
  }

  // -------------------------------------------------------------------
  class Master extends Actor {
    val queue         = Queue[(() => Any, ActorRef)]()
    var scheduledTick = false
    var started       = MutableList[Long]()

    import ExecutionContext.Implicits.global
    def receive = {
      case Work(func) =>
        if(queue.isEmpty && canStart()) {
          execWork(func, sender)
        } else {
          queue.enqueue((func,sender))
          if(!scheduledTick) {
            scheduledTick = true
            val nextstart = nextStart()
            system.scheduler.scheduleOnce(nextstart, self, Tick)
          }
        }
      case Tick =>
        while(!queue.isEmpty && canStart()) {
          val (func, from) = queue.dequeue()
          execWork(func, from)
        }
        if(!queue.isEmpty) {
          val nextstart = nextStart()
          system.scheduler.scheduleOnce(nextstart, self, Tick)
        } else {
          scheduledTick = false
        }
      case Stop =>
        context.stop(self)
    }

    def execWork(func: () => Any, from: ActorRef) = {
      started += timestamp() // Assumption: time always goes forward
      Future {
        func()
      } recover {
        case e: IllegalArgumentException => "blah"
      } pipeTo(from)
    }

    def canStart() = {
      val now = timestamp()
      started = started.filter(ts => (ts+s*1000) > now)
      started.size < n
    }

    def nextStart() = {
      canStart() match {
        case true  => 0.seconds
        case false => Math.max(started.head+s*1000-timestamp(), 0).milliseconds

      }
    }

    def timestamp() = System.currentTimeMillis
  }
}
