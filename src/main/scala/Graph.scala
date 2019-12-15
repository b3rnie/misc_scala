package misc
import scala.collection.mutable.{ListBuffer,Map,Queue}

class Graph[T <% Comparable[T]] {
  var vertices = ListBuffer[T]()
  var edges    = Map[T,List[(T,Int)]]()

  def addVertex(v: T) = {
    vertices += v
  }

  def addEdge(weight: Int, from: T, to: T) = {
    require(weight>=0)
    edges.get(from) match {
      case Some(l) => edges.put(from, l :+ (to,weight))
      case None    => edges.put(from, List((to,weight)))
    }
  }

  def dijkstra(source: T) = {
    var dist = Map[T,Int]((source,0))
    var prev = Map[T,T]()
    var q    = new MinBinHeap[Int,T]()
    q.insert(0, source)
    vertices.foreach(v => {
      if(v!=source) {
        dist(v) = -1
        // prev(v) = -1 //undefined
      }
      // q.insert()
      // q.enqueue(v)
    })
    while(q.size()>0) {
      var(_,u) = q.head()
      edges.get(u) match {
        case Some(l) =>
          l.foreach {
            case (v,weight) => {
              dist.get(u) match {
                case Some(du) if du>=0 =>
                  val alt = du + weight
                  dist.get(v) match {
                    case Some(dv) if alt < dv =>
                      dist(v) = alt
                      prev(v) = u
                      q.insert(alt, v)
                    case _ =>
                  }
                case _ =>
              }
            }
          }
        case None =>
      }
    }
    (dist,prev)
  }
}

