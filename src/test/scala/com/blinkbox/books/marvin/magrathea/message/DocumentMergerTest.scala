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
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    )
    val bookB = sampleBook(
      ("fieldB" -> "Value B") ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert(doc \ "fieldA" == JString("Value A"))
      assert(doc \ "fieldB" == JString("Value B"))
    }
    assert(result \ "source" \ "fieldB" != JNothing)
    assert(reverseResult \ "source" \ "fieldA" != JNothing)
  }

  it should "combine two book documents so that more recent information is emitted" in {
    val bookA = sampleBook(
      ("common_field" -> "Old Field") ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    )
    val bookB = sampleBook(
      ("common_field" -> "New Field!") ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert(doc \ "common_field" == JString("New Field!"))
    }
    assert(result \ "source" \ "common_field" != JNothing)
    assert(reverseResult \ "source" \ "common_field" == JNothing)
  }

  it should "refuse to combine two book documents with different classifications" in {
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
      ("trust" -> "Trusted Field") ~
      ("source" -> ("$remaining" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    )
    val bookB = sampleBook(
      ("trust" -> "Less Trusted Field") ~
      ("source" -> ("$remaining" -> ("role" -> "publisher_ftp") ~ ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert(doc \ "trust" == JString("Trusted Field"))
    }
    assert(result \ "source" \ "trust" == JNothing)
    assert(reverseResult \ "source" \ "trust" != JNothing)
  }

  it should "replace old data with new data, on two book documents, if it is from the same trusted source" in {
    val bookA = sampleBook(
      ("data" -> "A value") ~
      ("source" -> ("$remaining" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    )
    val bookB = sampleBook(
      ("data" -> "B value") ~
      ("source" -> ("$remaining" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert(doc \ "data" == JString("B value"))
    }
    assert(result \ "source" \ "data" != JNothing)
    assert(reverseResult \ "source" \ "data" == JNothing)
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
      assert(doc \ "things" \ "data" == JString(correctData))
    }
    assert(result \ "source" \ "things" != JNothing)
    assert(reverseResult \ "source" \ "things" == JNothing)
  }

  it should "merge two different keys with appropriate classifications" in {
    val bookA =
      ("source" -> ("a-ness" ->
        ("system" -> ("name" -> "marvin/design_docs") ~ ("version" -> "1.0.0")) ~
        ("role" -> "publisher_ftp") ~
        ("username" -> "jp-publishing") ~
        ("deliveredAt" -> DateTime.now.minusMinutes(1)) ~
        ("processedAt" -> DateTime.now.minusMinutes(1)))
      ) ~
      ("a-ness" ->
        ("classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))) ~
        ("data" -> "Item A")
      )
    val bookB =
      ("source" -> ("b-ness" ->
        ("system" -> ("name" -> "marvin/design_docs") ~ ("version" -> "1.0.0")) ~
        ("role" -> "publisher_ftp") ~
        ("username" -> "jp-publishing") ~
        ("deliveredAt" -> DateTime.now.plusMinutes(1)) ~
        ("processedAt" -> DateTime.now.plusMinutes(1)))
      ) ~
      ("b-ness" ->
        ("classification" -> List(("realm" -> "type") ~ ("id" -> "b-ness"))) ~
        ("data" -> "Item B")
      )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert(doc \ "a-ness" \ "data" == JString("Item A"))
      assert(doc \ "b-ness" \ "data" == JString("Item B"))
      assert(doc \ "source" \ "a-ness" != JNothing)
      assert(doc \ "source" \ "b-ness" != JNothing)
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
    assert(result \ "source" \ "thongs" != JNothing)
    assert(result \ "source" \ "things" == JNothing)
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
      assert(doc \ "things" \ "data" == JString("Newer"))
    }
    assert(result \ "things" \\ "source" != JObject(List.empty))
    assert(reverseResult \ "things" \\ "source" == JObject(List.empty))
  }

  it should "replace an older object with a newer one, and merge the unique ones" in {
    val bookA = sampleBook(
      ("a-cool" -> List("cool" -> "a","sweet" -> "2")) ~
      ("cool" -> List("t1" -> 1, "t2" -> 2)) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    )
    val bookB = sampleBook(
      ("b-cool" -> List("cool" -> "b", "sweet" -> "2")) ~
      ("cool" -> List("t3" -> 3, "t4" -> 4)) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      assert((doc \ "cool").children == List[JValue]("t3" -> 3, "t4" -> 4))
      assert((doc \ "a-cool").children == List[JValue]("cool" -> "a", "sweet" -> "2"))
      assert((doc \ "b-cool").children == List[JValue]("cool" -> "b", "sweet" -> "2"))
    }
    assert(result \ "source" \ "cool" != JNothing)
    assert(result \ "source" \ "b-cool" != JNothing)
    assert(reverseResult \ "source" \ "cool" == JNothing)
    assert(reverseResult \ "source" \ "a-cool" != JNothing)
  }

  it should "correctly merge object fields and sub keys should be treated as if they were parent keys" in {
    val bookA = sampleBook(
      ("a" -> ("b" -> "1ab")) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
    )
    val bookB = sampleBook(
      ("a" -> ("c" -> "2ac")) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now)))
    )
    val bookC = sampleBook(
      ("a" -> ("b" -> "3ab")) ~
      ("source" -> ("$remaining" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
    )
    val resultA = DocumentMerger.merge(bookA, bookB)
    val reverseResultA = DocumentMerger.merge(bookB, bookA)
    val resultB = DocumentMerger.merge(bookA, bookC)
    val reverseResultB = DocumentMerger.merge(bookC, bookA)
    Seq(resultA, reverseResultA).foreach { doc =>
      assert(doc \ "a" == (("b" -> "1ab") ~ ("c" -> "2ac")))
    }
    Seq(resultB, reverseResultB).foreach { doc =>
      assert(doc \ "a" == JObject(List[JField]("b" -> "3ab"): _*))
    }
  }
}
