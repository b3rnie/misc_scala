package misc
import scala.reflect.ClassTag

class BinHeap[T <% Ordered[T]](implicit m : ClassTag[T]) {
  val array = new Array[T](2000)
  var next  = 0

  def insert(v : T) = {
    // maybe expand array
    array(next) = v
    next += 1
    def bubble_up(pos : Int) : Unit = {
      pos match {
        case 0 =>
        case _ =>
          val d_pos = daddy(pos)
          val d_val = array(d_pos)
          if(d_val < v) {
            swap(pos, d_pos)
            bubble_up(d_pos)
          }
      }
    }
    bubble_up(next-1)
  }

  def head() : T = {
    val head  = array(0)
    val nhead = array(next-1)
    array(0)  = nhead
    next -= 1
    def bubble_down(pos : Int) : Unit = {
      val l_pos = l_kid(pos)
      val r_pos = r_kid(pos)
      val l_val = if (l_pos < next) {
        Option(array(l_pos))
      } else {
        None
      }
      val r_val = if (r_pos < next) {
        Option(array(r_pos))
      } else {
        None
      }
      (l_val, r_val) match {
        case (Some(v1), Some(v2)) if v1 >= v2 && v1 > nhead =>
          swap(l_pos, pos)
          bubble_down(l_pos)
        case (Some(v1), Some(v2)) if v2 >= v1 && v2 > nhead =>
          swap(r_pos, pos)
          bubble_down(r_pos)
        case (Some(v),  _)        if v > nhead =>
          swap(l_pos, pos)
          bubble_down(l_pos)
        case (_,        Some(v))  if v > nhead =>
          swap(r_pos, pos)
          bubble_down(r_pos)
        case _                             =>
      }
    }
    bubble_down(0)
    head
  }

  def peek() : T = array(0)

  def size() = next

  def swap(p1 : Int, p2 : Int) = {
    val tmp = array(p1)
    array(p1) = array(p2)
    array(p2) = tmp
  }
  def daddy(i : Int) = (i - 1) / 2
  def l_kid(i : Int) = 2 * i + 1
  def r_kid(i : Int) = 2 * i + 2
}
