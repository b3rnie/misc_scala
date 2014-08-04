import misc._
import org.scalatest._

class SimpleDslTest extends FlatSpec {
  "DSL" should "work" in {
    val s = """
    [RULENAME1]
    on_entry: e1, e2
    on_entry: e5, e6
    on_exit:  e3, e4
    out1 -> RULENAME_BLAH
    out2 -> RULENAME_XXX
    [RULENAME2]
    on_exit: e1,e2
    on_entry: x,y
    xxx -> ABC
    yyy -> DEF
    """
    println(SimpleDsl(s))
  }
}
