package com.humio.ingest.client

import java.util.concurrent.TimeUnit
import java.util.logging.Logger

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.ActorMaterializer
import HumioJsonProtocol._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
  * Created by chr on 17/11/2016.
  */

class HumioClient(hostUrl: String, dataspace: String, token: String) {
  
  val logger = LoggerFactory.getLogger(getClass)
  
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  
  val url = s"$hostUrl/api/v1/dataspaces/$dataspace/ingest"
  logger.info(s"creating humio client with url: $url")
  
  def put(events: Seq[TagsAndEvents]): Unit = {
    val entity = HttpEntity(contentType= ContentTypes.`application/json`, events.toJson.toString())
    val req = HttpRequest(method=HttpMethods.POST, uri = url, entity = entity).addCredentials(OAuth2BearerToken(token))
    val responseFuture: Future[HttpResponse] =  Http().singleRequest(req)
    logger.info(s"send request with size=${events.foldLeft(0){case (acc, tagsAndEvents) => acc + tagsAndEvents.events.size}} events")

    Await.ready(responseFuture, Duration(5, TimeUnit.SECONDS)).value.get match {
      case Success(response) => {
        if (!response.status.isSuccess()) {
          logger.error(s"error sending request to humio. status=${response.status.intValue()} res=${response.entity.toString}")
        }
        
      }
      case Failure(e) => {
        logger.error("error sending request to humio", e)
      }
    }
    
  }
  
}
