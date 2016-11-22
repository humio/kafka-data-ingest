package com.humio.ingest.main

import java.io.FileInputStream
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import com.humio.ingest.client.{Event, HumioClient}
import com.humio.ingest.kafka.{KafkaClient, KafkaDataProducer}
import spray.json.{JsObject, JsString}

/**
  * Created by chr on 22/11/2016.
  */
object ProducerRunner extends App{
  
  produce()
  
  def produce(): Unit = {
    val p1 = new KafkaDataProducer("localhost:9092", "test1")
    val p2 = new KafkaDataProducer("localhost:9092", "test3")
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
