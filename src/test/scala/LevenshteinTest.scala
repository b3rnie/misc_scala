import misc._
import org.scalatest._

class LevenshteinTest extends FlatSpec {
  "Levenshtein" should "work" in {
    assert(Levenshtein.distance("foo", "foo") === 0)
    assert(Levenshtein.distance("foo", "bar") === 3)
    assert(Levenshtein.distance("abc", "bac") === 2)
    assert(Levenshtein.distance("foo", "")    === 3)
    assert(Levenshtein.distance("kitten",
                                "sitting") === 3)
    assert(Levenshtein.distance("Saturday",
                                "Sunday") === 3)
  }
}
