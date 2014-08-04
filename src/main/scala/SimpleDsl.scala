import scala.collection.immutable.HashMap
import scala.util.parsing.combinator._

object SimpleDsl extends RegexParsers {
  def rs: Parser[List[Rule]] = rep(r)
  def r                      = rn ~ rep(ree) ~ rep(rt) ^^ {
    case name~ee~trans =>{
      var on_e = List[String]()
      var on_x = List[String]()
      ee.foreach{
        case ("on_entry",l) => on_e = on_e ::: l
        case ("on_exit", l) => on_x = on_x ::: l
        case e              => throw new Exception(e.toString)
      }
      var transitions = new HashMap[String,String]
      trans.foreach((t) =>{
        if(transitions.contains(t._1)) throw new Exception("duplicate: " + t._1)
        transitions += t
      })
      Rule(name        = name,
           on_entry    = on_e,
           on_exit     = on_x,
           transitions = transitions)
    }
  }
  def rn              = "[" ~> rulename <~ "]"
  def ree             = ("on_entry" | "on_exit") ~ ":" ~ repsep(name, ",") ^^ {
    case what~":"~list => (what, list)
  }
  def rt              = name ~ "->" ~ rulename ^^ {
    case name~"->"~rulename => (name,rulename)
  }
  def name            = """[a-z][a-z0-9_]*""".r
  def rulename        = """[A-Z][A-Z0-9_]*""".r

  def apply(input: String) = {
    parseAll(rs, input) match {
      case Success(result, _) => result
      case failure: NoSuccess => throw new Exception(failure.msg)
    }
  }
}

case class Rule(
  name:        String,
  on_entry:    List[String],
  on_exit:     List[String],
  transitions: Map[String,String]
)
