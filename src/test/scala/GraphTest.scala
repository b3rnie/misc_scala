import misc._
import org.scalatest._
import scala.concurrent._
import scala.concurrent.duration._

class GraphTest extends FlatSpec {
  "Dijkstra" should "work" in {
    val g = new Graph[Int]()
    g.addVertex(1)
    g.addVertex(2)
    g.addVertex(3)
    g.addVertex(4)
    g.addVertex(5)
    g.addVertex(6)

    g.addEdge(7, 1, 2)
    g.addEdge(9, 1, 3)
    g.addEdge(14, 1, 6)

    g.addEdge(7, 2, 1)
    g.addEdge(10, 2, 3)
    g.addEdge(15, 2, 4)

    g.addEdge(9, 3, 1)
    g.addEdge(10, 3, 2)
    g.addEdge(11, 3, 4)
    g.addEdge(2, 3, 6)

    g.addEdge(15, 4, 2)
    g.addEdge(11, 4, 3)
    g.addEdge(6,  4, 5)

    g.addEdge(6, 5, 4)
    g.addEdge(9, 5, 6)

    g.addEdge(14, 6, 1)
    g.addEdge(2,  6, 3)
    g.addEdge(9,  6, 5)

    println(g.dijkstra(1))
  }
}
