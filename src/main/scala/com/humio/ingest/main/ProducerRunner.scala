package com.humio.ingest.main

import java.time.format.DateTimeFormatter

import com.humio.ingest.kafka.KafkaDataProducer
import com.humio.ingest.producer.DataProducer
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
        val kafkaProducers = createKafkaProducers()
        //val (p1, p2) = createKafkaProducers()
        while(true) {
          try{
            
            for(kp <- kafkaProducers; 
                k <- 0 until 10) {
             kp.send(DataProducer.createData(i))
             requests += 1
            }
            
            val time = System.currentTimeMillis()
            if ((time - lastMeasuredTime) > 1000) {
              logger.info(s"produced $requests")
              requests = 0
              lastMeasuredTime = time
            }
            
            Thread.sleep(10)
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

  
  
  def createKafkaProducers(): Seq[KafkaDataProducer] = {
    for(i <- 0 until 1) yield {
      new KafkaDataProducer("localhost:8091", s"topic1")
    }
    //val kp1 = new KafkaDataProducer("localhost:9093", "test1")
    //val kp2 = new KafkaDataProducer("localhost:9093", "test2")
    //Seq(kp1, kp2)
  }

}
