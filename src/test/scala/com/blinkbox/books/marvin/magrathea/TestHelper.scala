package com.blinkbox.books.marvin.magrathea

import java.util.UUID

import com.blinkbox.books.marvin.magrathea.Helpers._
import com.blinkbox.books.marvin.magrathea.message.DocumentAnnotator
import com.blinkbox.books.spray.v2
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.JsonAST.{JNothing, JString, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods

import scala.language.implicitConversions
import scala.util.Random

trait TestHelper extends v2.JsonSupport with JsonMethods {
  val systemName = "bb-publishing"

  implicit def dateTime2JValue(d: DateTime): JString = JString(ISODateTimeFormat.dateTime().print(d.withZone(DateTimeZone.UTC)))

  def generateId = UUID.randomUUID()
  
  def getRandomIsbn(num: Int = Random.nextInt(1000000)) = s"9780007${"%06d".format(num)}"

  def sampleBook(extraContent: JValue = JNothing): JValue = {
    ("$schema" -> "ingestion.book.metadata.v2") ~
    ("classification" -> List(
      ("realm" -> "isbn") ~
      ("id" -> "9780111222333")
    )) ~
    ("source" ->
      ("system" ->
        ("name" -> "marvin/design_docs") ~
        ("version" -> "1.0.0")
      ) ~
      ("role" -> "publisher_ftp") ~
      ("username" -> systemName) ~
      ("deliveredAt" -> DateTime.now) ~
      ("processedAt" -> DateTime.now)
    ) merge extraContent
  }

  def sampleContributor(extraContent: JValue = JNothing): JValue = {
    ("$schema" -> "ingestion.contributor.metadata.v2") ~
    ("classification" -> List(
      ("realm" -> "isbn") ~
      ("id" -> "9780111222333")
    )) ~
    ("source" ->
      ("system" ->
        ("name" -> "marvin/design_docs") ~
        ("version" -> "1.0.0")
      ) ~
      ("role" -> "publisher_ftp") ~
      ("username" -> systemName) ~
      ("deliveredAt" -> DateTime.now) ~
      ("processedAt" -> DateTime.now)
    ) merge extraContent
  }

  def annotatedSampleBook(extraContent: JValue = JNothing): JValue =
    DocumentAnnotator.annotate(sampleBook(extraContent))

  def current(document: JValue): Current =
    extractFieldsFrom(document) match { case (schema, classification, doc, source) =>
      Current(UUID.randomUUID(), schema, classification, doc, source)
    }

  def history(document: JValue): History =
    extractFieldsFrom(document) match { case (schema, classification, doc, source) =>
      History(UUID.randomUUID(), schema, classification, doc, source)
    }
}
