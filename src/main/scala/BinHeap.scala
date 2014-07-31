package misc
import scala.reflect.ClassTag

abstract class BinHeap[K <% Ordered[K],V](it: Iterator[Tuple2[K,V]]) {
  var array = new Array[Tuple2[K,V]](101)
  var next  = 0
  while(it.hasNext) {
    maybe_expand_array()
    array(next) = it.next
    next += 1
  }
  if(next!=0) {
    (0 to next / 2).reverse.foreach(n => {
      bubble_down(n)
    })
  }

  def insert(k: K, v: V) = {
    maybe_expand_array()
    array(next) = Tuple2(k,v)
    next += 1
    def bubble_up(pos : Int) : Unit = {
      pos match {
        case 0 =>
        case _ =>
          val d_pos        = daddy(pos)
          val(d_key,d_val) = array(d_pos)
          if(compare(k, d_key)) {
            swap(pos, d_pos)
            bubble_up(d_pos)
          }
      }
    }
    bubble_up(next-1)
  }

  def head() : Tuple2[K,V] = {
    assert_not_empty()
    val head  = array(0)
    val(k, v) = array(next-1)
    array(0)  = array(next-1)
    next     -= 1
    bubble_down(0)
    maybe_shrink_array()
    head
  }

  def peek() : Tuple2[K,V] = {
    assert_not_empty()
    array(0)
  }

  def assert_not_empty() = {
    if(next == 0) {
      throw new Exception("queue is empty")
    }
  }

  def size() = next

  def bubble_down(pos: Int) : Unit = {
    val(k,v)  = array(pos)
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
      case (Some(Tuple2(k1,v1)), Some(Tuple2(k2,v2)))
        if (compare(k1, k2) || k1 == k2) && compare(k1, k) =>
        swap(l_pos, pos)
        bubble_down(l_pos)
      case (Some(Tuple2(k1,v1)), Some(Tuple2(k2,v2)))
        if (compare(k2, k1) || k2 == k1) && compare(k2, k) =>
        swap(r_pos, pos)
        bubble_down(r_pos)
      case (Some(Tuple2(k1,v1)),  _)
        if compare(k1, k) =>
        swap(l_pos, pos)
        bubble_down(l_pos)
      case (_, Some(Tuple2(k2,v2)))
        if compare(k2, k) =>
        swap(r_pos, pos)
        bubble_down(r_pos)
      case _ =>
    }
  }

  def maybe_expand_array() = {
    if(next == array.size) {
      val newarray = new Array[Tuple2[K,V]](array.size * 3)
      array.copyToArray(newarray)
      array = newarray
    }
  }
  def maybe_shrink_array() = {
    if(array.size > 101 && next < (array.size/6)) {
      val newarray = new Array[Tuple2[K,V]](array.size/3)
      array.copyToArray(newarray, 0, next)
      array = newarray
    }
  }
  def swap(p1: Int, p2: Int) = {
    val tmp = array(p1)
    array(p1) = array(p2)
    array(p2) = tmp
  }
  def daddy(i: Int) = (i - 1) / 2
  def l_kid(i: Int) = 2 * i + 1
  def r_kid(i: Int) = 2 * i + 2
  def compare(k1: K, k2: K) : Boolean
  override def toString() = {
    array.toString
  }
}

class MaxBinHeap[K <% Ordered[K], V](
    it: Iterator[Tuple2[K,V]]
  ) extends BinHeap[K,V](it) {

  def this() = this(Iterator[Tuple2[K,V]]())

  def compare(k1: K, k2: K) = k1 > k2
}

class MinBinHeap[K <% Ordered[K], V](
    it: Iterator[Tuple2[K,V]]
  ) extends BinHeap[K,V](it) {

  def this() = this(Iterator[Tuple2[K,V]]())

  def compare(k1: K, k2: K) = k1 < k2
}
