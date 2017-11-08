package com.humio.ingest.producer

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

object DataProducer {

  private val dt = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  def createData(randomNumber: Long): String = {
    if (Math.random() < 0.5) {
      createData1(randomNumber)
    } else {
      createData2(randomNumber)
    }
  }

  def createData1(randomNumber: Long): String = {
    val time = System.currentTimeMillis() / 1000D
    s"""
       |{
       |  "pipeline": "us1",
       |  "celery_identifier": "minimez",
       |  "level": "INFO",
       |  "ts": $time,
       |  "host": "compute${randomNumber % 10000}-sjc1",
       |  "msg": "this is my log message"
       |}
       |""".stripMargin
  }


  def createData2(randomNumber: Long): String = {
    s"""
       |{
       |  "pipeline": "us1",
       |  "celery_identifier": "minimez",
       |  "level": "INFO",
       |  "time": "${dt.format(ZonedDateTime.now())}",
       |  "host": "compute${randomNumber % 10000}-sjc1",
       |  "msg": "this is my log message"
       |}
       |""".stripMargin
  }
}
