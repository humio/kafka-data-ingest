package com.humio.ingest.main

import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.ArrayBlockingQueue

import com.humio.ingest.client.HumioClient
import com.humio.ingest.kafka.KafkaClient
import com.humio.ingest.kafka.KafkaClient.{ClusterAndTopic, OffsetHandling}
import com.humio.ingest.main.MessageHandler.MessageHandlerConfig

/**
  * Created by chr on 17/11/2016.
  */
object Runner extends App {
  
  run()

  def run(): Unit = {
    val kafkaClient = createKafkaClient()
    val humioClient = createHumioClient()
    val queues = kafkaClient.setupReadLoop()
    val messageHandler = createMessageHandler(queues, humioClient)
    
    
  }
  
  def createKafkaClient(): KafkaClient = {
    val properties = readPropertiesFromFile("./kafka-consumer.properties")
    val topics = readTopics("topics.txt")

    val humioProps = readPropertiesFromFile("./humio.properties")
    val offsetHandling = humioProps.getProperty("kafkaOffsetHandling") match {
      case str if (str != null && str.trim.length > 0) => OffsetHandling.withName(str)
      case _ => OffsetHandling.continue
    }
    new KafkaClient(properties, topics, offsetHandling)
  }
  
  private def readTopics(file: String): Map[String, List[String]] = {
    val lines = scala.io.Source.fromFile("topics.txt").getLines().map(_.trim).filterNot(s => s.isEmpty || s.startsWith("#"))
    lines.foldLeft(("", Map[String, List[String]]())) {
      case ((currentZookeeperStr, map), line) => {
        if (line.startsWith("@@")) {
          val currentZookeeperStr =  line.replaceAll("@", "")
          currentZookeeperStr -> map
        } else {
          val seq = map.getOrElse(currentZookeeperStr, List())
          currentZookeeperStr -> (map + (currentZookeeperStr -> (line :: seq)))
        }
      }
    }._2
  }
  
  def createHumioClient(): HumioClient = {
    val props = readPropertiesFromFile("./humio.properties")
    val httpThreads = getProperty(props, "humioHttpThreads", "10").toInt
    new HumioClient(props.getProperty("hostUrl"), props.getProperty("dataspace"), Option(props.getProperty("token")), httpThreads)
  }
  
  def createMessageHandler(queues: Map[ClusterAndTopic, ArrayBlockingQueue[String]], humioClient: HumioClient): MessageHandler = {
    val props = readPropertiesFromFile("./humio.properties")
    val config = MessageHandlerConfig(
                                      maxByteSize = getProperty(props, "maxByteSize", "1048576").toInt,
                                      maxWaitTimeSeconds = getProperty(props, "maxWaitTimeSeconds", "1").toInt
    )
    new MessageHandler(queues, humioClient, config)
  }
  
  def getProperty(properties: Properties, key: String, defaultValue: String) : String = {
    val value = properties.getProperty(key)
    if (value != null) value else defaultValue 
  }
  
  def readPropertiesFromFile(filename: String): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream(filename))
    properties
  }
  
}
