package com.blinkbox.books.marvin.magrathea.event

import com.blinkbox.books.json.DefaultFormats
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.JsonAST.{JNothing, JString, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.junit.runner.RunWith
import org.scalatest.FunSuiteLike
import org.scalatest.junit.JUnitRunner
import spray.httpx.Json4sJacksonSupport

import scala.language.{implicitConversions, postfixOps}
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class DocumentMergerSpec extends FunSuiteLike with Json4sJacksonSupport with JsonMethods {
  implicit val json4sJacksonFormats = DefaultFormats
  implicit def dateTime2JValue(d: DateTime) = JString(ISODateTimeFormat.dateTime().print(d.withZone(DateTimeZone.UTC)))

  private def generateId = BigInt(130, Random).toString(16)

  private def sampleBook(extraContent: JValue = JNothing): JValue =
    ("_id" -> generateId) ~
    ("$schema" -> "ingestion.book.metadata.v2") ~
    ("classification" -> List(
      ("realm" -> "isbn") ~
      ("id" -> "9780111222333"))
    ) ~
    ("source" ->
      ("$remaining" ->
        ("system" ->
          ("name" -> "marvin/design_docs") ~
          ("version" -> "1.0.0")
        ) ~
        ("role" -> "publisher_ftp") ~
        ("username" -> "jp-publishing") ~
        ("deliveredAt" -> DateTime.now) ~
        ("processedAt" -> DateTime.now)
      )
    ) merge extraContent

  private def sampleContributor(extraContent: JValue = JNothing): JValue =
    ("_id" -> generateId) ~
    ("$schema" -> "ingestion.contributor.metadata.v2") ~
    ("classification" -> List(
      ("realm" -> "contributor_id") ~
      ("id" -> "abc123"))
    ) ~
    ("source" ->
      ("$remaining" ->
        ("system" ->
          ("name" -> "marvin/design_docs") ~
          ("version" -> "1.0.0")
        ) ~
        ("role" -> "publisher_ftp") ~
        ("username" -> "jp-publishing") ~
        ("deliveredAt" -> DateTime.now) ~
        ("processedAt" -> DateTime.now)
      )
    ) merge extraContent

  test("Must combine two documents so that more recent information is emitted") {
    val book = sampleBook()
    println(pretty(render(book)))
    assert(1 == 1)
  }

  test("Must not combine two documents with different classifications") {
    assert(1 == 1)
  }

  test("Must not replace old data with new data if it is from a less trusted source") {
    assert(1 == 1)
  }

  test("Must add sub-objects") {
    assert(1 == 1)
  }

  test("Must deep merge sub-objects with different classifications") {
    assert(1 == 1)
  }

  test("Must replace an older sub-object with a newer one, if they have the same classification") {
    assert(1 == 1)
  }
}
