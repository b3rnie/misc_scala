package misc
import scala.collection.Iterator

object Sample {
  def reservoirSample[T: Manifest](iterator: Iterator[T], size: Integer): Seq[T] = {
    var out = new Array[T](size)
    val r = scala.util.Random
    iterator.foldLeft((0, new Array(size))) {
      case ((idx,array), e) =>
        if(idx < size) {
          array(idx) = e
        } else {
          val j = r.nextInt(idx)
          if (j < size)
            array(j) = e
        }
        (idx+1, array)
    }._2.toSeq
  }
}
