package com.blinkbox.books.marvin.magrathea

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import org.json4s.JsonAST.{JNothing, JValue}
import org.json4s.jackson.JsonMethods
import spray.httpx.Json4sJacksonSupport

object Helpers extends Json4sJacksonSupport with JsonMethods {
  override implicit val json4sJacksonFormats = DefaultFormats

  def extractFieldsFrom(document: JValue): (String, JValue, JValue, JValue) = {
    val schema = document \ "$schema"
    val classification = document \ "classification"
    val source = document \ "source"
    if (schema == JNothing || classification == JNothing || source == JNothing) throw new IllegalArgumentException(
      s"Cannot find document schema, classification and source: ${compact(render(document))}")
    val doc = document.removeDirectField("$schema").removeDirectField("classification").removeDirectField("source")
    (schema.extract[String], classification, doc, source)
  }
}
