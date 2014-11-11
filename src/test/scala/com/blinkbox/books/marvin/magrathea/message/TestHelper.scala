package com.blinkbox.books.marvin.magrathea.message

import java.util.UUID

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.Helpers._
import com.blinkbox.books.marvin.magrathea.{History, Latest}
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.JsonAST.{JNothing, JString, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import spray.httpx.Json4sJacksonSupport

import scala.language.implicitConversions
import scala.util.Random

trait TestHelper extends Json4sJacksonSupport with JsonMethods {
  override implicit val json4sJacksonFormats = DefaultFormats

  implicit def dateTime2JValue(d: DateTime) = JString(ISODateTimeFormat.dateTime().print(d.withZone(DateTimeZone.UTC)))

  def generateId = BigInt(130, Random).toString(16)

  def getRandomIsbn(num: Int = Random.nextInt(1000000)) = s"9780007${"%06d".format(num)}"

  def sampleBook(extraContent: JValue = JNothing, includeId: Boolean = true): JValue = {
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
      ("username" -> "jp-publishing") ~
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
      ("username" -> "jp-publishing") ~
      ("deliveredAt" -> DateTime.now) ~
      ("processedAt" -> DateTime.now)
    ) merge extraContent
  }

  def annotatedSampleBook(extraContent: JValue = JNothing): JValue =
    DocumentAnnotator.annotate(sampleBook(extraContent))

  def latest(document: JValue): Latest =
    withFields(document) match { case (schema, classification, doc, source) =>
      Latest(UUID.randomUUID(), schema, classification, doc, source)
    }

  def history(document: JValue): History =
    withFields(document) match { case (schema, classification, doc, source) =>
      History(UUID.randomUUID(), schema, classification, doc, source)
    }
}
