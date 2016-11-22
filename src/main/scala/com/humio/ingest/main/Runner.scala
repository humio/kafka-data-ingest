package com.humio.ingest.main

import java.io.FileInputStream
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import com.humio.ingest.client.{Event, HumioClient}
import com.humio.ingest.kafka.{KafkaClient, KafkaDataProducer}
import spray.json.{JsObject, JsString, JsValue}

/**
  * Created by chr on 17/11/2016.
  */
object Runner extends App {
  
  read(readConsumerProperties(), readTopics())
  
  def readTopics(): Seq[String] = {
    scala.io.Source.fromFile("topics.txt").getLines().map(_.trim).toSeq
  }
  
  def readConsumerProperties(): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream("./kafka-consumer-properties.txt"))
    properties
  }
  
  def read(properties: Properties, topics: Seq[String]): Unit = {
    new KafkaClient(properties, topics, 2).setupReadLoop()
  }
}
