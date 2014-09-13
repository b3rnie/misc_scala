package misc

class BinTree[K <% Ordered[K],V]() {
  class Node(var k: K, var v: V) {
    var left:  Option[Node] = None
    var right: Option[Node] = None
  }

  var root: Option[Node] = None

  def insert(k: K, v: V) = {
    root match {
      case None    => root = Option(new Node(k,v))
      case Some(n) => doInsert(n,k,v)
    }
  }

  def doInsert(n: Node, k: K, v: V) : Unit = {
    k.compare(n.k) match {
      case x if x > 0 && n.right.isEmpty => n.right = Option(new Node(k,v))
      case x if x > 0                    => doInsert(n.right.get,k,v)
      case x if x < 0 && n.left.isEmpty  => n.left = Option(new Node(k,v))
      case x if x < 0                    => doInsert(n.left.get, k, v)
      case 0                             => n.v = v
    }
  }

  def remove(k: K) : Option[V] = doRemove(root, None, k)

  def doRemove(e: Option[Node], daddy: Option[Node], k: K) : Option[V] = {
    e match {
      case None               => None
      case Some(n) if n.k < k => doRemove(n.right, e, k)
      case Some(n) if n.k > k => doRemove(n.left, e, k)
      case Some(n)            =>
        var ret = n.v
        (n.left, n.right) match {
          case (Some(nl), Some(nr)) =>
            var successor = nr
            while(successor.left != None) {
              successor = successor.left.get
            }
            doRemove(n.right, e, successor.k)
            n.k = successor.k
            n.v = successor.v
          case (Some(nl), None)     => replaceInParent(daddy, n.left, e)
          case (None,     Some(nr)) => replaceInParent(daddy, n.right, e)
          case (None,     None)     => replaceInParent(daddy, None, e)
        }
        Option(ret)
    }
  }

  def replaceInParent(daddy:     Option[Node],
                      newNode:   Option[Node],
                      childNode: Option[Node]) = {
    daddy match {
      case None    => root = newNode
      case Some(n) =>
        if(n.left.eq(childNode)) n.left = newNode
        else n.right = newNode
    }
  }

  def lookup(k: K) : Option[V] = doLookup(root,k)

  def doLookup(e: Option[Node], k: K) : Option[V] = {
    e match {
      case None               => None
      case Some(n) if n.k < k => doLookup(n.right, k)
      case Some(n) if n.k > k => doLookup(n.left, k)
      case Some(n)            => Option(n.v)
    }
  }

  def print() = {
    def do_print(n: Option[Node]) : Unit = {
      n match {
        case Some(e) =>
          println("{" + e.k + "," + e.v + "}")
          if(!e.left.isEmpty) do_print(e.left)
          if(!e.right.isEmpty) do_print(e.right)
        case None =>
      }
    }
    do_print(root)
  }
}
