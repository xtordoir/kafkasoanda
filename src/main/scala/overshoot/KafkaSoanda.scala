package io.kensu

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream._ 
import org.apache.kafka.streams.Consumed

import com.lightbend.kafka.scala._
import com.lightbend.kafka.scala.streams._

import java.util.Properties
import java.util.concurrent.CountDownLatch

import play.api.libs.json._

import io.kensu.hff._

import com.typesafe.config.ConfigFactory
import collection.JavaConverters._
import scala.collection.JavaConversions._

object KafkaSoanda extends App {
  import JsonRW._

  val props = new Properties()

  val conf = ConfigFactory.load()//.root()//.withOnlyKey("kafka")

  val scales: List[Double] = conf.getDoubleList("scales").asScala.map(_.doubleValue).toList

  println(scales)

  val config = conf.root().withOnlyKey("kafka").toConfig()
  //println(conf.root().withOnlyKey("kafka").render())
  config.entrySet.foreach {e => 
    props.setProperty(e.getKey().replaceAll("^kafka.", ""), config.getString(e.getKey()))
  }


  
//  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "overshoots")
//  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//  props.put("auto.offset.reset", "earliest")
//  props.put("commit.interval.ms", "250")
//  props.put("processing.guarantee", "exactly_once")
  //props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "org.apache.kafka.streams.processor.WallclockTimestampExtractor")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass())
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass())
  
  val builder = new StreamsBuilderS()
  // read "pricing" topic and deserialize as PriceMsg objects
  val source: KStreamS[String, PriceMsg] = builder.stream("pricing", 
                                                          Consumed.`with`(
                                                            Serdes.String(),
                                                            HFFSerdes.priceMsgSerde
                                                          )
                                                        )
                                                  /*.through("pricemsg", Produced.`with`(
                                                    Serdes.String(),
                                                    HFFSerdes.priceMsgKryoSerde))*/

  def getScale(k: String): Double = k.replaceAll("""^(.*)_""","").toDouble
  // groupby instrument
  // aggregate by computing the Overshoot
  // and embed the previous hashCode so we can filter out consecutive duplicates
  val osKTables = source.flatMap{(k: String, v: PriceMsg) => 
                           scales.map{scale => {
                            //println(s"${k}_${scale} -- $v")
                            (s"${k}_${scale}", v)
                          }
                            }
                         }
                         .groupByKey(Serialized.`with`(Serdes.String(), HFFSerdes.priceMsgKryoSerde))
                         .aggregate( () => ("".hashCode, Overshoot(1)), 
                                         (k: String, v: PriceMsg, tup: (Int, Overshoot)) => {
                                             val overs = tup._2.update(Tick(v.bid, v.ask), getScale(k))
                                             //println(s"aggregated into overs")
                                             val hash = tup._2.hashCode
                                             (hash, overs)
                                             }
                                             ,
                                         Materialized.as(s"tmp-dupl-overshoot")
                                                    .withValueSerde(
                                                    HFFSerdes.deduplOvershootSerde)                 
                                         )
                        
  val oss = osKTables.toStream
                // filter consecutive duplicates 
                .filter((k: String, tup: (Int, Overshoot)) => tup._1 != tup._2.hashCode)
                // remove previous hashCode
                .mapValues( tup =>  
                  tup._2
                 )
  
  val osRaws = oss.through("overshoots", Produced.`with`(Serdes.String(),
                                      HFFSerdes.overshootJsonSerde))

             
  val dc = osRaws.groupByKey(Serialized.`with`(Serdes.String(), HFFSerdes.overshootJsonSerde))
       .aggregate( () => (0, DChange(1, 0, 0)),
                         (k: String, os: Overshoot, tup: (Int, DChange)) => {
                           val newDC = tup._2.update(os, os.scale)
                           val newDir = tup._2.direction
                           (newDir, newDC)
                         }
                         ,Materialized.as("tmp-dupl-dchange")
                                                    .withValueSerde(
                                                    HFFSerdes.deduplDChangeSerde)                 
                                         )
       .toStream
       .filter((k: String, tup: (Int, DChange)) => tup._1 != tup._2.direction)
       .mapValues( tup =>  
                  tup._2
                 )  
  dc.to("dchange", Produced.`with`(Serdes.String(),
                                      HFFSerdes.dChangeJsonSerde))
                                      
//  os.inner.foreach((k: String, v: Overshoot) => {
//    println(s"${k}: ${v} => ${v.ext/v.prevExt}")
//    //println(s"${k} ---- ${v}"))
//    }
//    )
    
  
  val topology = builder.build()
  
  //println(topology.describe())
  
  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()
}
