package com.humio.ingest.client

import java.nio.charset.StandardCharsets
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.util.logging.Logger

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.ActorMaterializer
import HumioJsonProtocol._
import akka.Done
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.slf4j.LoggerFactory
import spray.json._

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
  * Created by chr on 17/11/2016.
  */

class HumioClient(hostUrl: String, dataspace: String, token: Option[String], httpThreads: Int) {
  
  val logger = LoggerFactory.getLogger(getClass)
  
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  
  val url = s"$hostUrl/api/v1/dataspaces/$dataspace/ingest"
  logger.info(s"creating humio client with url: $url")
  
  val http = Http()

  val httpSendQueue = new ArrayBlockingQueue[TagsAndEvents](httpThreads)
  
  init()
  
  def send(events: TagsAndEvents): Unit = {
    httpSendQueue.put(events)
  }
  
  private def init(): Unit = {
    for(i <- 0 until httpThreads) {
      new Thread() {
        override def run(): Unit = {
          loop()    
          logger.info("looping")
        }
      }.start()
    }
  }
  
  private def loop(): Unit = {
    val events = httpSendQueue.poll(100, TimeUnit.MILLISECONDS)
    if (events != null) {

      val fut = sendHttp(events)
      try {
        Await.ready(fut, Duration(30, TimeUnit.SECONDS)).value.get
      } catch {
        case e: Throwable => logger.error("error waiting for http request", e)
      }
    }
    
    loop() : @tailrec
  }
  
  private def sendHttp(events: TagsAndEvents, retry: Int = 0): Future[Done] = {
    try {
      val json = Seq(events).toJson.toString()
      val entity = HttpEntity(contentType = ContentTypes.`application/json`, json)
      val r = HttpRequest(method = HttpMethods.POST, uri = url, entity = entity)
      val req =
        token match {
          case Some(t) => r.addCredentials(OAuth2BearerToken(t))
          case None => r
        }
      val responseFuture: Future[HttpResponse] = http.singleRequest(req)
      val eventSize = events.events.size
      val byteSize = json.getBytes(StandardCharsets.UTF_8).size
      val time = System.currentTimeMillis()

      responseFuture.onComplete{
        case Success(response) => {
          if (!response.status.isSuccess()) {
            logger.error(s"error sending request to humio. status=${response.status.intValue()} res=${response.entity.toString}")
          }
          response.discardEntityBytes()
          logger.info(s"request finished. time=${System.currentTimeMillis() - time}, events=$eventSize size=$byteSize")

        }
        case Failure(e) => {
          fail(e, events, retry)
        }
      }
      responseFuture.map(r => Done)
    } catch {
      case ex: Exception => {
        fail(ex, events, retry)
      }
    }
  }
  
  private def fail(exception: Throwable, events: TagsAndEvents, retry: Int): Future[Done] = {
    if (retry <= 2) {
      logger.error("Got exception sending request to humio. will retry...", exception)
      sendHttp(events, retry + 1)
    } else {
      logger.error("Got exception sending request to humio. not retrying", exception)
      Future.successful(Done)
    }
  }
  
}
