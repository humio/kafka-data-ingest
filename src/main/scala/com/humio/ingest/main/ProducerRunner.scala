package com.humio.ingest.main

import com.humio.ingest.kafka.{KafkaClient, KafkaDataProducer}
import org.slf4j.LoggerFactory

/**
  * Created by chr on 22/11/2016.
  */
object ProducerRunner extends App{

  val logger = LoggerFactory.getLogger(getClass)
  
  produce()
  
  def produce(): Unit = {
    new Thread() {
      override def run(): Unit = {
        var i = 0L
        var j = Long.MaxValue / 2
        val (p1, p2) = createKafkaProducers()
        while(true) {
          try{
            val data1 = createData(i)
            p1.send(data1)

            val data2 = createData(j)
            p2.send(data2)
            
            Thread.sleep(1)
          } catch {
            case e: Throwable => {
              logger.error("error producing", e)
              produce()
            }
          }
          i += 1
          j += 1
        }
      }
    }.start()
  }
  
  def createData(randomNumber: Long): String = {
    val time = System.currentTimeMillis() / 1000D
    s"""
       |{
       |  "pipeline": "us1",
       |  "celery_identifier": "minimez",
       |  "level": "INFO",
       |  "ts": $time,
       |  "host": "compute${randomNumber % 10000}-sjc1",
       |  "msg": "this is my log message"
       |}
              """.stripMargin
  }
  
  def createKafkaProducers(): (KafkaDataProducer, KafkaDataProducer) = {
    val kp1 = new KafkaDataProducer("localhost:9092", "test1")
    val kp2 = new KafkaDataProducer("localhost:9092", "test2")
    kp1 -> kp2
  }

}
