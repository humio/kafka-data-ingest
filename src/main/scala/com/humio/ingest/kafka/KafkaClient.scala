package com.humio.ingest.kafka

import java.util
import java.util.Properties

import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.javaapi.consumer.ConsumerConnector
import kafka.serializer.StringDecoder
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by chr on 17/11/2016.
  */
class KafkaClient(externalProperties: Properties, topics: Seq[String], threadsPerTopic: Int) {

  val logger = LoggerFactory.getLogger(getClass)
  
  private def setupConsumer(): ConsumerConnector = {
    val props = new Properties()
    props.put("group.id", "humio-log-reader")
    props.put("fetch.message.max.bytes", s"${1024 * 1024 * 10}")
    
    props.putAll(externalProperties)
    
    val config: ConsumerConfig = new ConsumerConfig(props)
    logger.info(s"creating consumer with properties=${props}")
    Consumer.createJavaConsumerConnector(config)
    
  }

  def setupReadLoop(): Unit = {
    val consumer = setupConsumer()
    val topicStreamMap = new util.HashMap[String, Integer]()
    for(topic <- topics) {
      topicStreamMap.put(topic, new java.lang.Integer(threadsPerTopic))  
    }
    val res = consumer.createMessageStreams(topicStreamMap, new StringDecoder(), new StringDecoder())
    
    logger.info(s"creating message streams: ${res.asScala.map(t => (t._1, t._2.toList))}")
    
    for( (topic, streams) <- res;
         stream <- streams) {
      new Thread() {
        override def run(): Unit = {
          try {
            val it = stream.iterator()
            logger.info(s"creating stream for topic=$topic stream=${stream.clientId}")
            while(it.hasNext()) {
              logger.info(s"iterator hasNext was true for topic=$topic, stream=$stream")
              val msg = it.next().message()
              logger.info(s"read message from topic=$topic msg=$msg")
            }
            logger.info(s"leaving read loop for topic=$topic stream=$stream")
          } catch {
            case ex: Throwable => {
              logger.error(s"Error consuming topic=$topic", ex)
              Thread.sleep(5000)
            }
          }
        }
      }.start()
    }
  }
}
