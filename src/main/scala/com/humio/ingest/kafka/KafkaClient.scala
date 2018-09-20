package com.humio.ingest.kafka

import java.util
import java.util.Properties
import java.util.concurrent.ArrayBlockingQueue

import com.humio.ingest.kafka.KafkaClient.OffsetHandling.OffsetHandling
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.javaapi.consumer.ConsumerConnector
import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConversions._


object KafkaClient {
  object OffsetHandling extends Enumeration {
    type OffsetHandling = Value
    val continue, back, now = Value
  }
  
  case class ClusterAndTopic(zookeeperUrl: String, topic: String)
  
}

import KafkaClient._
/**
  * Created by chr on 17/11/2016.
  */
class KafkaClient(externalProperties: Properties, topics: Map[String, Seq[String]], offsetHandling: OffsetHandling) {

  val logger = LoggerFactory.getLogger(getClass)
  
  val groupID = "humio-log-reader"
  
  private def setupConsumer(zookeeperConnectStr: String): ConsumerConnector = {
    val props = new Properties()
    props.put("group.id", groupID)
    props.putAll(externalProperties.asInstanceOf[util.Hashtable[_, _]])
    props.put("zookeeper.connect", zookeeperConnectStr)

    def deleteConsumerGroup(): Unit = {
      val consumerGroupPath = s"/consumers/${groupID}"
      val zkClient = new ZkClient(zookeeperConnectStr, 5000)
      if (zkClient.exists(consumerGroupPath)) {
        logger.info(s"deleting consumerGroup=$groupID in zookeeper by deleting path=$consumerGroupPath")
        zkClient.deleteRecursive(consumerGroupPath)
      }
    }
    
    offsetHandling match {
      case OffsetHandling.continue =>
      case OffsetHandling.back => {
        props.put("auto.offset.reset", "smallest")
        deleteConsumerGroup()
      }
      case OffsetHandling.now => {
        props.put("auto.offset.reset", "largest")
        deleteConsumerGroup()
      }
    }
    
    val config: ConsumerConfig = new ConsumerConfig(props)
    logger.info(s"creating consumer with properties=$props")
    Consumer.createJavaConsumerConnector(config)
  }

  def setupReadLoop(): Map[ClusterAndTopic, ArrayBlockingQueue[String]] = {
    var queues = Map[ClusterAndTopic, ArrayBlockingQueue[String]]()
    for((zookeeperConnectStr, topics) <- topics) {
      val consumer = setupConsumer(zookeeperConnectStr)
      
      val topicStreamMap = new util.HashMap[String, Integer]()
      for (topic <- topics) {
        topicStreamMap.put(topic, 1)
        logger.info(s"listening to topic: $zookeeperConnectStr -> $topic")
        queues += ClusterAndTopic(zookeeperUrl = zookeeperConnectStr, topic = topic) -> new ArrayBlockingQueue[String](10)
      }
      val res = consumer.createMessageStreams(topicStreamMap, new StringDecoder(), new StringDecoder())

      for ((topic, streams) <- res;
           stream <- streams) {
        val clusterAndTopic = ClusterAndTopic(zookeeperConnectStr, topic)
        new Thread() {
          override def run(): Unit = {
            logger.info(s"starting thread for topic: $topic")
            try {
              val queue = queues(clusterAndTopic)
              val it = stream.iterator()
              while (it.hasNext()) {
                val msg = it.next().message()
                queue.put(msg)
              }
              run() : @tailrec
            } catch {
              case ex: Throwable => {
                logger.error(s"Error consuming topic=$topic", ex)
                Thread.sleep(1000)
              }
            }
          }
        }.start()
      }
    }
    queues
  }
}
