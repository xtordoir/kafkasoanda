package io.kensu.hff

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.Deserializer
import java.io.ByteArrayOutputStream
import com.twitter.chill.ScalaKryoInstantiator
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import scala.reflect.ClassTag

import play.api.libs.json._
import play.api.libs.functional.syntax._
import java.text.SimpleDateFormat
import java.util.Date

import scala.reflect.runtime.universe.typeOf
import scala.reflect.runtime.universe._
/**
object to instantiate Kryo with chill

*/

object KryoInst {
  val instantiator = new ScalaKryoInstantiator
  instantiator.setRegistrationRequired(false)
  val kryo = instantiator.newKryo()
}

/** 
Generic class to generate a Kafka Serializer with Kryo for any case class
*/

case class HFFSerializer[T]()(implicit tag: ClassTag[T]) extends Serializer[T] {
  import KryoInst._
  
  def close(): Unit = ()
  def configure(x$1: java.util.Map[String, _],x$2: Boolean): Unit = ()

  def serialize(topic: String, obj: T): Array[Byte] = {
    //println(s"Kryo serializing $obj onto $topic")
   if (obj == null) {
      null
    }
  else {
    val out = new ByteArrayOutputStream()
    val output = new Output(out)
    kryo.writeObject(output, obj)
    output.close()
    out.toByteArray()
  }
  }
}

/** 
Generic class to generate a Kafka Deserializer with Kryo for any case class
*/
case class HFFDeserializer[T  >: Null](implicit tag: ClassTag[T]) extends Deserializer[T] {
  import KryoInst._

  def close(): Unit = ()
  def configure(x$1: java.util.Map[String, _],x$2: Boolean): Unit = ()


  def deser(topic: String, data: Array[Byte]): T = {
    //println(s"Kryo Deserializing $data from $topic")

    if (data == null || data.size < 1) {
      null
     } else { 
      val input = new Input(data)
      val ret = kryo.readObject(input, tag.runtimeClass.asInstanceOf[Class[T]]).asInstanceOf[T]
      //println(ret)
      ret
    }
  }

  def deserialize(topic: String, data: Array[Byte]): T = {
    deser(topic, data)
  }
}
/**
 PriceMsg messages provided by Oanda pricing are in JSON format

*/
object JsonRW {
  val fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

 // implicit val liquidityReads = Json.reads[Liquidity]
  implicit val liquidityReads: Reads[Liquidity] = (
    (JsPath \ "price").read[String].map(_.toDouble) and
    (JsPath \ "liquidity").read[Long]
  )(Liquidity.apply _)

  implicit val priceMsgReads: Reads[PriceMsg] = (
    (JsPath \ "time").read[String].map{ x => 
                                            //println(x)
                                             x.replaceAll("""\d{6}Z""", "")
                                      }.map(fmt.parse(_)) and
    (JsPath \ "bids").read[List[Liquidity]].map(_.head.price) and
    (JsPath \ "asks").read[List[Liquidity]].map(_.head.price)
  )(PriceMsg.apply _)

  implicit val overshootFormat =  Json.format[Overshoot]
  implicit val dChangeFormat =  Json.format[DChange]

  def toJson[T](obj: T) = obj match {
    case o: Overshoot => Some(Json.toJson(o))
    case o: DChange => Some(Json.toJson(o))
    case _ => None
  } 

  def parse[T: TypeTag](msg: String)(implicit tag: TypeTag[T]) = tag match {
    case t if tag.tpe =:= typeOf[Overshoot] => Json.parse(msg).as[Overshoot]
    case t if tag.tpe =:= typeOf[DChange] => Json.parse(msg).as[DChange]
    case _ => null
  }
}

case class PriceMsgDeserializer() extends Deserializer[PriceMsg] {
  import JsonRW._
  def close(): Unit = ()
  def configure(x$1: java.util.Map[String, _],x$2: Boolean): Unit = ()
  def deserialize(topic: String, data: Array[Byte]): PriceMsg = {
    val msg = new String(data, "UTF-8")
    //println(s"Json parsing + $msg from $topic")
    Json.parse(msg).as[PriceMsg]
  }
}

case class HFFJsonSerializer[T](implicit tag: ClassTag[T]) extends Serializer[T] {
  import JsonRW._
  def close(): Unit = ()
  def configure(x$1: java.util.Map[String, _],x$2: Boolean): Unit = ()

  def serialize(topic: String, obj: T): Array[Byte] = {
    if (obj == null) {
      null
    }
    else {
      val out = new ByteArrayOutputStream()
      val js = toJson(obj).map(Json.stringify).getOrElse(null)
      js.getBytes
    }
  }
}

case class HFFJsonDeserializer[T]()(implicit tag: TypeTag[T]) extends Deserializer[T] {
  import JsonRW._
  def close(): Unit = ()
  def configure(x$1: java.util.Map[String, _],x$2: Boolean): Unit = ()
  def deserialize(topic: String, data: Array[Byte]): T = {
    val msg = new String(data, "UTF-8")
    JsonRW.parse[T](msg).asInstanceOf[T]//(tag)
  }  

}

/**
HFFSerdes defines Kafka Serdes for our entities
*/

object HFFSerdes {

  def priceMsgSerde() = Serdes.serdeFrom(HFFSerializer[PriceMsg](), 
                                        PriceMsgDeserializer())

  def priceMsgKryoSerde() = Serdes.serdeFrom(HFFSerializer[PriceMsg](), 
                                        HFFDeserializer[PriceMsg]())

  def overshootSerde() = Serdes.serdeFrom(HFFSerializer[Overshoot](), 
                                          HFFDeserializer[Overshoot]())

  def deduplOvershootSerde() = Serdes.serdeFrom(HFFSerializer[(Int, Overshoot)](), 
                                          HFFDeserializer[(Int, Overshoot)]())

  def overshootJsonSerde() = Serdes.serdeFrom(HFFJsonSerializer[Overshoot](), 
                                          HFFJsonDeserializer[Overshoot]())

  def dChangeSerde() = Serdes.serdeFrom(HFFSerializer[DChange](), 
                                          HFFDeserializer[DChange]()) 

  def dChangeJsonSerde() = Serdes.serdeFrom(HFFJsonSerializer[DChange](), 
                                          HFFJsonDeserializer[DChange]())   

  def deduplDChangeSerde() = Serdes.serdeFrom(HFFSerializer[(Int, DChange)](), 
                                          HFFDeserializer[(Int, DChange)]())
}
