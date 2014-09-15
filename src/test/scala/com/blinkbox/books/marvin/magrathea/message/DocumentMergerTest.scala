package com.blinkbox.books.marvin.magrathea.message

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.message.DocumentMerger.DifferentClassificationException
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.junit.runner.RunWith
import org.scalatest.FlatSpecLike
import org.scalatest.junit.JUnitRunner
import spray.httpx.Json4sJacksonSupport

import scala.language.{implicitConversions, postfixOps}
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class DocumentMergerTest extends FlatSpecLike with Json4sJacksonSupport with JsonMethods {
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

  "The document merger" should "combine two book documents with two unique keys" in {
    val bookA = sampleBook(
      ("fieldA" -> "Value A") ~
      ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    )
    val bookB = sampleBook(
      ("fieldB" -> "Value B") ~
      ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert((doc \ "fieldA").extract[String] == "Value A")
      assert((doc \ "fieldB").extract[String] == "Value B")
    }
  }

  it should "combine two book documents so that more recent information is emitted" in {
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
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert((doc \ "field").extract[String] == "New Field!")
    }
  }

  it should "combine two book documents with different classifications" in {
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
    intercept[DifferentClassificationException] {
      DocumentMerger.merge(bookB, bookA)
    }
  }

  it should "not replace old data with new data, on two book documents, if it is from a less trusted source" in {
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
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert((doc \ "field").extract[String] == "Trusted Field")
    }
  }

  it should "replace old data with new data, on two book documents, if it is from the same trusted source" in {
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
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert((doc \ "field").extract[String] == "B value")
    }
  }

  it should "add sub-objects on two book documents" in {
    val bookA = sampleBook("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    val correctData = "Whatever"
    val bookB = sampleBook(
      ("things" -> List(("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~ ("data" -> correctData))) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert((doc \ "things").children.size == 1)
      assert((doc \ "things" \ "data").extract[String] == correctData)
    }
  }

  it should "merge two different keys with appropriate classifications" in {
    val aNess = "a-ness"
    val bNess = "b-ness"
    val bookA =
      ("source" ->
        (aNess ->
          ("system" -> ("name" -> "marvin/design_docs") ~ ("version" -> "1.0.0")) ~
          ("role" -> "publisher_ftp") ~
          ("username" -> "jp-publishing") ~
          ("deliveredAt" -> DateTime.now.minusMinutes(1)) ~
          ("processedAt" -> DateTime.now.minusMinutes(1))
        )
      ) ~
      (aNess ->
        ("classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))) ~
        ("data" -> "Item A")
      )
    val bookB =
      ("source" ->
        (bNess ->
          ("system" -> ("name" -> "marvin/design_docs") ~ ("version" -> "1.0.0")) ~
          ("role" -> "publisher_ftp") ~
          ("username" -> "jp-publishing") ~
          ("deliveredAt" -> DateTime.now.plusMinutes(1)) ~
          ("processedAt" -> DateTime.now.plusMinutes(1))
        )
      ) ~
      (bNess ->
        ("classification" -> List(("realm" -> "type") ~ ("id" -> "b-ness"))) ~
        ("data" -> "Item B")
      )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert((doc \ aNess \ "data").extract[String] == "Item A")
      assert((doc \ bNess \ "data").extract[String] == "Item B")
      assert((doc \ "source" \ aNess).children.size == 5)
      assert((doc \ "source" \ bNess).children.size == 5)
    }
  }

  it should "deep merge the same sub-objects on two book documents with different classifications" in {
    val bookA = sampleBook(
      "things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))) ~ ("data" -> "Item A"))
    )
    val bookB = sampleBook(
      "things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "b-ness"))) ~ ("data" -> "Item B"))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert((doc \ "things").children.size == 2)
      assert((doc \ "things" \\ "data").children.contains(JString("Item A")))
      assert((doc \ "things" \\ "data").children.contains(JString("Item B")))
    }
  }

  it should "deep merge different sub-objects on two book documents with different classifications" in {
    val bookA = sampleBook(
      "things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))) ~ ("data" -> "Item A"))
    )
    val bookB = sampleBook(
      "thongs" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "b-ness"))) ~ ("data" -> "Item B"))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert((doc \ "things").children.size == 1)
      assert((doc \ "thongs").children.size == 1)
    }
  }

  it should "replace an older sub-object with a newer one, on two book documents, if they have the same classification" in {
    val bookA = sampleBook(
      ("things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "p-ness"))) ~ ("data" -> "Older"))) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    )
    val bookB = sampleBook(
      ("things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "p-ness"))) ~ ("data" -> "Newer"))) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert((doc \ "things").children.size == 1)
      assert((doc \ "things" \ "data").extract[String] == "Newer")
    }
  }
}
