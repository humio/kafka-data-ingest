package com.humio.ingest.main

import com.humio.ingest.kafka.KafkaClient.ClusterAndTopic
import com.humio.ingest.main.Runner.createHumioClient
import com.humio.ingest.producer.DataProducer
import org.slf4j.LoggerFactory

import scala.util.Random

object DataForHttpRunner extends App{

  val logger = LoggerFactory.getLogger(getClass)

  val humioClient = createHumioClient()
  
  run()
  
  def run(): Unit = {
    val clusterAndTopics =
      for(i <- 0 until 500) yield {
        ClusterAndTopic(zookeeperUrl = s"zookeeper1${i % 2}", topic =s"host$i")
      }
    val random = new Random()
    
    val eventsInReqeust = sys.env.getOrElse("EVENTS", "10000").toInt
    
    while(true) {
      for(tag <- clusterAndTopics) {
        val data = 
          for(i <- 0 until random.nextInt(eventsInReqeust)) yield {
            DataProducer.createData(random.nextLong())
          }
        val tagsAndEvents = MessageHandler.transformJson(tag, data)
        val t = System.currentTimeMillis()
        humioClient.send(tagsAndEvents)
        logger.info(s"send events=${tagsAndEvents.events.size} time=${System.currentTimeMillis() - t}")
      }
      Thread.sleep(1000)
    }
  }
  
}
