package misc

object Levenshtein {
  // copied from http://en.wikipedia.org/wiki/Wagner-Fischer_algorithm
  def distance(s: String, t: String) : Int = {
    // For all i and j, d[i,j] will hold the Levenshtein distance between
   // the first i characters of s and the first j characters of t.
   // Note that d has (m+1)  x(n+1) values.
    val d = Array.ofDim[Int](s.length+1, t.length+1)

    // set each element to zero
    (0 to s.length).foreach(i =>
      (0 to t.length).foreach(j => d(i)(j) = 0)
    )

    // the distance of any first string to an empty second string
    (0 to s.length).foreach(i =>
      d(i)(0) = i
    )
    // the distance of any second string to an empty first string
    (0 to t.length).foreach(j =>
      d(0)(j) = j
    )

    (1 to t.length).foreach(j => {
      (1 to s.length).foreach(i => {
        if(s(i-1) == t(j-1)) {
          d(i)(j) = d(i-1)(j-1) // no operation required
        } else {
          d(i)(j) = min(d(i-1)(j) + 1,  // a deletion
                        d(i)(j-1) + 1,  // an insertion
                        d(i-1)(j-1) + 1 // a substitution
                      )
        }
      })
    })
    d(s.length)(t.length)
  }

  def min(a: Int, b: Int, c:Int) = {
    List(a,b,c).reduceLeft(math.min)
  }
}
