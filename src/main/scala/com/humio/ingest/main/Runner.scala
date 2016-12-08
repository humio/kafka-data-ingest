package com.humio.ingest.main

import java.io.FileInputStream
import java.util.Properties

import com.humio.ingest.client.HumioClient
import com.humio.ingest.kafka.KafkaClient
import com.humio.ingest.main.MessageHandler.{Message, MessageHandlerConfig}

/**
  * Created by chr on 17/11/2016.
  */
object Runner extends App {
  
  run()

  def run(): Unit = {
    val kafkaClient = createKafkaClient()
    val humioClient = createHumioClient()
    val messageHandler = createMessageHandler(humioClient)

    val messageHandlerFun = (m: Message) => messageHandler.newMessage(m)
    kafkaClient.setupReadLoop(messageHandlerFun)

  }
  
  def createKafkaClient(): KafkaClient = {
    val properties = readPropertiesFromFile("./kafka-consumer.properties")
    val topics = scala.io.Source.fromFile("topics.txt").getLines().map(_.trim).toSeq
    new KafkaClient(properties, topics, 2)
  }
  
  def createHumioClient(): HumioClient = {
    val props = readPropertiesFromFile("./humio.properties")
    new HumioClient(props.getProperty("hostUrl"), props.getProperty("dataspace"), props.getProperty("token"))
  }
  
  def createMessageHandler(humioClient: HumioClient): MessageHandler = {
    val props = readPropertiesFromFile("./humio.properties")
    val config = MessageHandlerConfig(bulkSize = props.getProperty("bulkSize").toInt, queueSize = props.getProperty("queueSize").toInt, workerThreads = props.getProperty("workerThreads").toInt)
    new MessageHandler(humioClient, config)
  }
  
  def readPropertiesFromFile(filename: String): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream(filename))
    properties
  }
  
}
