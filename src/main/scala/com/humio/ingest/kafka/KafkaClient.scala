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
    
    Consumer.createJavaConsumerConnector(new ConsumerConfig(props))
    
  }

  def setupReadLoop(): Unit = {
    val consumer = setupConsumer()
    val topicStreamMap = new util.HashMap[String, Integer]()
    for(topic <- topics) {
      topicStreamMap.put(topic, threadsPerTopic)  
    }
    val res = consumer.createMessageStreams(topicStreamMap, new StringDecoder(), new StringDecoder())
    
    for( (topic, streams) <- res;
         stream <- streams) {
      new Thread() {
        override def run(): Unit = {
          try {
            val it = stream.iterator()
            while(it.hasNext()) {
              val msg = it.next().message()
              logger.info(s"read message from topic=$topic msg=$msg")
            }
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
