package com.humio.ingest.main

import java.time.format.DateTimeFormatter

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
        var lastMeasuredTime = System.currentTimeMillis()
        var requests = 0
        var i = 0L
        var j = Long.MaxValue / 2
        val (p1, p2) = createKafkaProducers()
        while(true) {
          try{
            val data1 = createData1(i)
            p1.send(data1)
            requests += 1

            val data2 = createData2(j)
            p2.send(data2)
            requests += 1
            
            val time = System.currentTimeMillis()
            if ((time - lastMeasuredTime) > 1000) {
              logger.info(s"produced $requests")
              requests = 0
              lastMeasuredTime = time
            }
            
            //Thread.sleep(2)
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


  private val isoDateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
  
  def createData1(randomNumber: Long): String = {
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
       |""".stripMargin
  }

  def createData2(randomNumber: Long): String = {
    val time = System.currentTimeMillis() / 1000D
    s"""
       |{
       |  "pipeline": "us1",
       |  "celery_identifier": "minimez",
       |  "level": "INFO",
       |  "time": "2017-03-24T09:41:09Z",
       |  "host": "compute${randomNumber % 10000}-sjc1",
       |  "msg": "this is my log message"
       |}
       |""".stripMargin
  }

  
  
  def createKafkaProducers(): (KafkaDataProducer, KafkaDataProducer) = {
    val kp1 = new KafkaDataProducer("localhost:9093", "test1")
    val kp2 = new KafkaDataProducer("localhost:9093", "test2")
    kp1 -> kp2
  }

}
