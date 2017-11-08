package com.humio.ingest.main

import java.nio.charset.StandardCharsets
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import spray.json._
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

import com.humio.ingest.client.{Event, HumioClient, TagsAndEvents}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/**
  * Created by chr on 06/12/2016.
  */
object MessageHandler{
  
  val logger = LoggerFactory.getLogger(getClass)
  
  case class MessageHandlerConfig(maxByteSize: Int, maxWaitTimeSeconds: Int)

  private val isoDateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  def transformJson(topic: String, messages: Seq[String]): TagsAndEvents = {
    val events =
      messages.flatMap {msg =>
        try {
          val json = msg.parseJson.asJsObject

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
          Some(event)
        } catch {
          case e: Throwable => {
            logger.error(s"Could not handle event for topic=${topic} msg=${msg}", e)
            None
          }
        }
      }

    TagsAndEvents(tags = Map("topic" -> topic), events = events)
  }
}

import MessageHandler._
class MessageHandler(queues: Map[String, ArrayBlockingQueue[String]], humioClient: HumioClient, config: MessageHandlerConfig) {
  
  startWorkers()
  
  private def startWorkers(): Unit = {
    for ((topic, queue) <- queues) {
      val waitTime = config.maxWaitTimeSeconds * 1000
      val t = new Thread() {
        override def run(): Unit = {
          try {
            val list = ArrayBuffer[String]()
            var byteSize = 0
            var firstEventTimestamp = -1L
            while (true) {
              val msg = queue.poll(100, TimeUnit.MILLISECONDS)
              if (msg != null) {
                val msgSize = msg.getBytes(StandardCharsets.UTF_8).size
                list += msg
                byteSize += msgSize
                if (firstEventTimestamp <=0) {
                  firstEventTimestamp = System.currentTimeMillis()
                }
                if (config.maxByteSize <= byteSize || firstEventTimestamp < (System.currentTimeMillis() - waitTime)) {
                  //val t = System.currentTimeMillis()
                  send(topic, list)
                  //logger.info(s"sending data to humio. size=${byteSize} events=${list.size} time=${System.currentTimeMillis() - t}")

                  list.clear()
                  byteSize = 0
                  firstEventTimestamp = -1
                }
              }
            }
          } catch {
            case e: Throwable => {
              logger.error("error in worker", e)
              Thread.sleep(5000)
              run() : @tailrec
            }
          }
        }
      }
      t.start()
    }
  }
  
  private def send(topic: String, messages: Seq[String]): Unit = {
    val tagsAndEvents = transformJson(topic, messages)
    humioClient.send(tagsAndEvents)
  }
}
