package example

import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonSerializer, ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

case class SomethingWeird (
  a1: String,
  a2: String
)

class CustomSerializer extends JsonSerializer[SomethingWeird] {
  override def serialize(value: SomethingWeird, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    gen.writeString(value.a1 + ':' + value.a2)
  }
}

class CustomDeSerializer extends JsonDeserializer[SomethingWeird] {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): SomethingWeird = {
    p.getValueAsString.split(":") match {
      case Array(a1, a2) => SomethingWeird(a1, a2)
    }
  }
}
object Weekday extends Enumeration {
  type Weekday = Value
  val Mon = Value("monday")
  val Tue = Value("tuesday")
}

class WeekdayType extends TypeReference[Weekday.type]

case class Message (
  basic: Int,
  @JsonScalaEnumeration(classOf[WeekdayType]) weekday: Weekday.Weekday,
  custom: SomethingWeird,
  @JsonProperty("nameX") nameY: String,
  list: Seq[Int]
)

case class SimpleMessage (
  foo: String
)

object Example extends App {

  val mapper = new ObjectMapper()
  mapper.registerModule(new DefaultScalaModule())
  mapper.registerModule(new SimpleModule()
                          .addSerializer(classOf[SomethingWeird], new CustomSerializer)
                          .addDeserializer(classOf[SomethingWeird], new CustomDeSerializer))
  val f = new File("/home/bernie/scala/json/test1.json")

  val result: Message = mapper.readValue(f, classOf[Message])
  println(mapper.writeValueAsBytes(result))
  println(result)
}
