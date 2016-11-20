package com.humio.ingest.main

import java.io.FileInputStream
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import com.humio.ingest.client.{Event, HumioClient}
import com.humio.ingest.kafka.{KafkaClient, KafkaDataProducer}
import spray.json.{JsObject, JsString, JsValue}

/**
  * Created by chr on 17/11/2016.
  */
object Runner extends App {
  
  //testHumioClient()
  //produce()
  //Thread.sleep(5000)
  
  read(readConsumerProperties(), readTopics())
  
  def readTopics(): Seq[String] = {
    scala.io.Source.fromFile("topics.txt").getLines().map(_.trim).toSeq
  }
  
  def readConsumerProperties(): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream("./kafka-consumer-properties.txt"))
    properties
  }
  
  def read(properties: Properties, topics: Seq[String]): Unit = {
    new KafkaClient(properties, topics, 2).setupReadLoop()
  }

  def produce(): Unit = {
    val p1 = new KafkaDataProducer("localhost:9092", "test1")
    val p2 = new KafkaDataProducer("localhost:9092", "test2")
    new Thread() {
      override def run(): Unit = {
        var i = 0
        var j = 0
        while(true) {
          p1.send(s"hello1 $i")
          Thread.sleep(1000)
          p2.send(s"hello2 $j")
          Thread.sleep(1000)
          i += 1
          j += 1
        }
      }
    }.start()
  }
  
  def testHumioClient(): Unit = {
    val client = new HumioClient("http://localhost:8080", "developer", "developer")

    val isoDateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
    
    val events = for(i <- 0 until 10) yield{
      Event(isoDateTimeFormatter.format(ZonedDateTime.now()), new JsObject(Map("hello" -> JsString(i.toString))))
    }
    
    client.put(events, Map("service" -> "chr"))
  }

}
