package com.humio.ingest.client

import spray.json.{DefaultJsonProtocol, JsObject}

/**
  * Created by chr on 17/11/2016.
  */
object HumioJsonProtocol extends DefaultJsonProtocol {

  implicit val eventFormat = jsonFormat2(Event)
  implicit val tagsAndEvents = jsonFormat2(TagsAndEvents)
}


case class TagsAndEvents(tags: Map[String, String], events: Seq[Event])
case class Event(timestamp: String, attributes: JsObject)