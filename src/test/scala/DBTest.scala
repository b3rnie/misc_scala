import misc._
import org.scalatest._

class DBTest extends FlatSpec {
  "DB" should "work" in {
    val db = new DB("test", "test")
    clean(db)
    db.put("foo", "bar".toArray.map(_.toByte))
    db.put("bar", "baz".toArray.map(_.toByte))
    assert("bar" == new String(db.get("foo").get))
    assert("baz" == new String(db.get("bar").get))
    assert(true  == db.delete("foo"))
    assert(None  == db.get("foo"))
    db.update("bar", "xxx".toArray.map(_.toByte))
    assert("xxx" == new String(db.get("bar").get))
    clean(db)
  }
  def clean(db: DB) = {
    db.list().foreach(db.delete(_))
  }
}
