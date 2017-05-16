package com.humio.ingest.main

import java.nio.charset.StandardCharsets
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import spray.json._
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

import com.humio.ingest.client.{Event, HumioClient, TagsAndEvents}
import org.slf4j.LoggerFactory

/**
  * Created by chr on 06/12/2016.
  */
object MessageHandler{
  
  val logger = LoggerFactory.getLogger(getClass)
  
  case class Message(jsonString: String, topic: String)
  case class MessageHandlerConfig(maxByteSize: Int, maxWaitTimeSeconds: Int, queueSize: Int, workerThreads: Int)
}

import MessageHandler._
class MessageHandler(humioClient: HumioClient, config: MessageHandlerConfig) {
  
  val queue = new ArrayBlockingQueue[Message](config.queueSize)
  startWorkers()
  
  
  def newMessage(msg: Message): Unit = {
    queue.put(msg)
  }

  private def startWorkers(): Unit = {
    for (_ <- 0 until config.workerThreads) {
      val t = new Thread() {
        override def run(): Unit = {
          var list = List[Message]()
          var byteSize = 0
          var time = System.currentTimeMillis()
          while (true) {
            try {
              val msg = queue.poll(100, TimeUnit.MILLISECONDS)
              val msgTime = System.currentTimeMillis()
              val msgSize = 
                if (msg != null) {
                  msg.jsonString.getBytes(StandardCharsets.UTF_8).size
                } else {
                  0
              }
              val waitTime = msgTime - time 
              if ((byteSize + msgSize) > config.maxByteSize ||
                waitTime > config.maxWaitTimeSeconds * 1000) {
                val sendTime = System.currentTimeMillis()
                if (!list.isEmpty) {
                  send(list)
                  logger.info(s"send request with events=${list.size} bytes=$byteSize, waitTime=$waitTime, requesttime=${System.currentTimeMillis() - sendTime}")
                }
                list = List()
                byteSize = 0
              }
              if (msg != null) {
                if (byteSize == 0) {
                  //we are adding the first new message
                  time = msgTime
                }
                list = msg :: list
                byteSize += msgSize
              }
            } catch {
              case e: Throwable => {
                logger.error("error in worker", e)
              }
            }
          }
        }
      }
      t.start()
    }
  }
  
  private def send(messages: Seq[Message]): Unit = {
    val tagsAndEvents = transformJson(messages)
    humioClient.send(tagsAndEvents)
  }

  private val isoDateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
  
  private def transformJson(msgs: Seq[Message]): Seq[TagsAndEvents] = {
    var res = Map[String, Seq[Event]]()
    for(msg <- msgs) {
      try {
        val json = msg.jsonString.parseJson.asJsObject

        val ts: Long =
          json.getFields("ts", "time") match {
            case Seq(JsNumber(time)) => (time.doubleValue() * 1000).toLong
            case Seq(JsString(dateTimeStr)) => ZonedDateTime.parse(dateTimeStr, isoDateTimeFormatter).toInstant.toEpochMilli
            case _ => {
              logger.warn(s"Got event without recognized timestamp. event=${json}")
              System.currentTimeMillis()
            }
          }

        val event = Event(ts, JsObject(json.fields))

        val seq = res.getOrElse(msg.topic, Seq())
        res += msg.topic -> ( seq :+ event)
      } catch {
        case e: Throwable => {
          logger.error(s"Could not handle event for topic=${msg.topic} msg=${msg}", e)
        }
      }
    }
    
    res.foldLeft(Vector[TagsAndEvents]()) { case (acc, (k,v)) => 
      acc :+ TagsAndEvents(tags = Map("topic" -> k), events = v)
    }
  }
}
