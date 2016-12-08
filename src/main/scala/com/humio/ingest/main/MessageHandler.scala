package com.humio.ingest.main

import scala.collection.JavaConversions._
import spray.json._
import com.humio.ingest.client.HumioJsonProtocol._
import java.util
import java.util.concurrent.ArrayBlockingQueue

import com.humio.ingest.client.{Event, HumioClient, TagsAndEvents}
import org.slf4j.LoggerFactory

/**
  * Created by chr on 06/12/2016.
  */
object MessageHandler{
  
  val logger = LoggerFactory.getLogger(getClass)
  
  case class Message(jsonString: String, topic: String)
  case class MessageHandlerConfig(bulkSize: Int, queueSize: Int, workerThreads: Int)
}

import MessageHandler._
class MessageHandler(humioClient: HumioClient, config: MessageHandlerConfig) {
  
  val queue = new ArrayBlockingQueue[Message](config.queueSize)
  startWorkers()
  
  
  def newMessage(msg: Message): Unit = {
    queue.put(msg)
  }

  private def startWorkers(): Unit = {
    for(_ <- 0 until config.workerThreads) {
      val t = new Thread() {
        override def run(): Unit = {
          val list = new util.ArrayList[Message](config.bulkSize)
          while (true) {
            try {
              queue.drainTo(list, config.bulkSize)
              if (!list.isEmpty) {
                val tagsAndEvents = transformJson(list)
                humioClient.put(tagsAndEvents)
                list.clear()  
              } else {
                Thread.sleep(100)
              }
              Thread.sleep(3000)
              
            } catch {
              case e: Throwable => {
                logger.error("error in worker", e)
                Thread.sleep(5000)
              }
            }
          }
        }
      }
      t.start()
    }  
  }
  
  private def transformJson(msgs: Seq[Message]): Seq[TagsAndEvents] = {
    var res = Map[String, Seq[Event]]()
    for(msg <- msgs) {
      val json = msg.jsonString.parseJson
      val fields = json.asJsObject.fields
      
      val ts = fields.get("ts").get.asInstanceOf[JsNumber].value.doubleValue()
      val timestamp = (ts * 1000).toLong
      val attributes = fields - "ts"
      val event = Event(timestamp, JsObject(attributes))
      
      val seq = res.getOrElse(msg.topic, Seq())
      res += msg.topic -> ( seq :+ event)
    }
    
    res.foldLeft(Vector[TagsAndEvents]()) { case (acc, (k,v)) => 
      acc :+ TagsAndEvents(tags = Map("topic" -> k), events = v)
    }
  }
}
