package misc

import scala.slick.driver.H2Driver.simple._
import java.sql.{DriverManager}

class DB(name: String, tab: String) {
  Class.forName("org.postgresql.Driver")
  var url  = "jdbc:postgresql://localhost/" + name
  val user = "bernie"
  val pass = "foo"
  val conn = DriverManager.getConnection(url, user, pass)

  def get(key: String) : Option[Array[Byte]] = {
    val ps = conn.prepareStatement(s"SELECT val FROM $tab WHERE key = ?")
    ps.setString(1, key)
    val rs = ps.executeQuery()
    val res = if (rs.next()) {
      Option(rs.getBytes(1))
    } else {
      None
    }
    rs.close()
    ps.close()
    res
  }

  def put(k: String, v: Array[Byte]) = {
    val ps = conn.prepareStatement(s"INSERT INTO $tab(key, val) VALUES(?,?)")
    ps.setString(1, k)
    ps.setBytes(2, v)
    ps.executeUpdate()
    ps.close()
  }

  def update(k: String, v: Array[Byte]) = {
    val ps = conn.prepareStatement(s"UPDATE $tab SET val = ? WHERE key = ?")
    ps.setBytes(1, v)
    ps.setString(2, k)
    ps.executeUpdate()
    ps.close()
  }

  def delete(k: String) : Boolean = {
    val st = conn.prepareStatement(s"DELETE FROM $tab WHERE key = ?")
    st.setString(1, k)
    val res = st.executeUpdate()
    st.close()
    res >= 1
  }

  def list() : List[String] = {
    val ps = conn.prepareStatement(s"SELECT key FROM $tab")
    val rs = ps.executeQuery()
    val res = scala.collection.mutable.ListBuffer[String]()
    while (rs.next()) {
      res.append(rs.getString(1))
    }
    rs.close()
    ps.close()
    res.toList
  }
}
