package com.humio.ingest.kafka

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringEncoder


/**
  * Created by chr on 17/11/2016.
  */
class KafkaDataProducer(connectString: String, topic: String) {

  val kafkaProducer = createProducer(connectString)
  
  private def createProducer(connectString: String): Producer[String, String] = {
    val props = new Properties()
    props.put("metadata.broker.list", s"${connectString}")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    new Producer(new ProducerConfig(props))
  }
  
  def send(data: String): Unit = {
    kafkaProducer.send(new KeyedMessage(topic, data))
  }

}
