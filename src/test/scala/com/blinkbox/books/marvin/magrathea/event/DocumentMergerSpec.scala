package com.blinkbox.books.marvin.magrathea.event

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.event.DocumentMerger.DifferentClassificationException
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.JsonAST._
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
      ("id" -> "9780111222333")
    )) ~
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

  test("Must combine two book documents so that more recent information is emitted") {
    val bookA = sampleBook(
      ("field" -> "Old Field") ~
      ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    )
    val bookB = sampleBook(
      ("field" -> "New Field!") ~
      ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    assert((result \ "field").extract[String] == "New Field!")
  }

  test("Must not combine two book documents with different classifications") {
    val bookA = sampleBook(
      ("field" -> "Field") ~
      ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    )
    val bookB = sampleBook(
      ("field" -> "A different thing") ~
      ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "a different id"))) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    intercept[DifferentClassificationException] {
      DocumentMerger.merge(bookA, bookB)
    }
  }

  test("Must not replace old data with new data, on two book documents, if it is from a less trusted source") {
    val bookA = sampleBook(
      ("field" -> "Trusted Field") ~
      ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~
      ("source" -> ("$remaining" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    )
    val bookB = sampleBook(
      ("field" -> "Less Trusted Field") ~
      ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~
      ("source" -> ("$remaining" -> ("role" -> "publisher_ftp") ~ ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    assert((result \ "field").extract[String] == "Trusted Field")
  }

  test("Must not replace old data with new data, on two book documents, if it is from a less trusted source (inverted)") {
    val bookA = sampleBook(
      ("field" -> "Trusted Field") ~
        ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~
        ("source" -> ("$remaining" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    )
    val bookB = sampleBook(
      ("field" -> "Less Trusted Field") ~
        ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~
        ("source" -> ("$remaining" -> ("role" -> "publisher_ftp") ~ ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val result = DocumentMerger.merge(bookB, bookA)
    assert((result \ "field").extract[String] == "Trusted Field")
  }

  test("Must replace old data with new data, on two book documents, if it is from the same trusted source") {
    val bookA = sampleBook(
      ("field" -> "A value") ~
        ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~
        ("source" -> ("$remaining" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    )
    val bookB = sampleBook(
      ("field" -> "B value") ~
        ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~
        ("source" -> ("$remaining" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val result = DocumentMerger.merge(bookB, bookA)
    assert((result \ "field").extract[String] == "B value")
  }

  test("Must add sub-objects on two book documents") {
    val bookA = sampleBook("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    val correctData = "Whatever"
    val bookB = sampleBook(
      ("things" -> List(("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~ ("data" -> correctData))) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    assert((result \ "things").children.size == 1)
    assert((result \ "things" \ "data").extract[String] == correctData)
  }

  test("Must deep merge the same sub-objects on two book documents with different classifications") {
    val bookA = sampleBook(
      "things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))) ~ ("data" -> "Item A"))
    )
    val bookB = sampleBook(
      "things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "b-ness"))) ~ ("data" -> "Item B"))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    assert((result \ "things").children.size == 2)
  }
//
//  test("Must deep merge different sub-objects on two book documents with different classifications") {
//    val bookA = sampleBook(
//      "things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))) ~ ("data" -> "Item A"))
//    )
//    val bookB = sampleBook(
//      "thongs" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "b-ness"))) ~ ("data" -> "Item B"))
//    )
//    val result = DocumentMerger.merge(bookA, bookB)
//    assert((result \ "things").children.size == 1)
//    assert((result \ "thongs").children.size == 1)
//  }
//
//  test("Must replace an older sub-object with a newer one, on two book documents, if they have the same classification") {
//    val bookA = sampleBook(
//      ("things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "p-ness"))) ~ ("data" -> "Older"))) ~
//      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
//    )
//    val bookB = sampleBook(
//      ("things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "p-ness"))) ~ ("data" -> "Newer"))) ~
//      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
//    )
//    val result = DocumentMerger.merge(bookA, bookB)
//    assert((result \ "things").children.size == 1)
//    assert((result \ "things" \ "data").extract[String] == "Newer")
//  }
}
