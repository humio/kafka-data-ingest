package com.humio.ingest.kafka

import java.util
import java.util.Properties

import com.humio.ingest.main.MessageHandler.Message
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.javaapi.consumer.ConsumerConnector
import kafka.serializer.StringDecoder
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by chr on 17/11/2016.
  */
class KafkaClient(externalProperties: Properties, topics: Map[String, Seq[String]], threadsPerTopic: Int) {

  val logger = LoggerFactory.getLogger(getClass)
  
  private def setupConsumer(zookeeperConnectStr: String): ConsumerConnector = {
    val props = new Properties()
    props.put("group.id", "humio-log-reader")
    props.put("fetch.message.max.bytes", s"${1024 * 1024 * 10}")
    props.putAll(externalProperties)
    props.put("zookeeper.connect", zookeeperConnectStr)
    
    val config: ConsumerConfig = new ConsumerConfig(props)
    logger.info(s"creating consumer with properties=$props")
    Consumer.createJavaConsumerConnector(config)
  }

  def setupReadLoop(handleMessage: Message => Unit): Unit = {
    for((zookeeperConnectStr, topics) <- topics) {
      val consumer = setupConsumer(zookeeperConnectStr)
      
      val topicStreamMap = new util.HashMap[String, Integer]()
      for (topic <- topics) {
        topicStreamMap.put(topic, new java.lang.Integer(threadsPerTopic))
        logger.info(s"listening to topic: $zookeeperConnectStr -> $topic")
      }
      val res = consumer.createMessageStreams(topicStreamMap, new StringDecoder(), new StringDecoder())

      for ((topic, streams) <- res;
           stream <- streams) {
        new Thread() {
          override def run(): Unit = {
            try {
              val it = stream.iterator()
              while (it.hasNext()) {
                val msg = it.next().message()
                handleMessage(Message(msg, topic))
              }
              run()
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
}
