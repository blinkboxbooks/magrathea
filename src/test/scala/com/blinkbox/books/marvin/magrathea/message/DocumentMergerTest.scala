package com.blinkbox.books.marvin.magrathea.message

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.Json4sExtensions._
import com.blinkbox.books.marvin.magrathea.message.DocumentMerger.DifferentClassificationException
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.junit.runner.RunWith
import org.scalatest.{Matchers, FlatSpecLike}
import org.scalatest.junit.JUnitRunner
import spray.httpx.Json4sJacksonSupport

import scala.language.{implicitConversions, postfixOps}
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class DocumentMergerTest extends FlatSpecLike with Json4sJacksonSupport with JsonMethods with Matchers {
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
      ("system" ->
        ("name" -> "marvin/design_docs") ~
        ("version" -> "1.0.0")
      ) ~
      ("role" -> "publisher_ftp") ~
      ("username" -> "jp-publishing") ~
      ("deliveredAt" -> DateTime.now) ~
      ("processedAt" -> DateTime.now)
    ) merge extraContent

  "The document merger" should "combine two book documents with two unique keys" in {
    val bookA = sampleBook(
      ("fieldA" -> "Value A") ~
      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    )
    val bookB = sampleBook(
      ("fieldB" -> "Value B") ~
      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    val srcAHash = (bookA \ "source").sha1
    val srcBHash = (bookB \ "source").sha1
    Seq(result, reverseResult).foreach { doc =>
      result \ "fieldA" \ "value" shouldEqual JString("Value A")
      result \ "fieldA" \ "source" shouldEqual srcAHash
      result \ "fieldB" \ "value" shouldEqual JString("Value B")
      result \ "fieldB" \ "source" shouldEqual srcBHash
      result \ "source" \ srcAHash shouldEqual bookA \ "source"
      result \ "source" \ srcBHash shouldEqual bookB \ "source"
    }
  }

//  it should "combine two book documents so that more recent information is emitted" in {
//    val bookA = sampleBook(
//      ("commonField" -> "Old Field") ~
//      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
//    )
//    val bookB = sampleBook(
//      ("commonField" -> "New Field!") ~
//      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
//    )
//    val result = DocumentMerger.merge(bookA, bookB)
//    val reverseResult = DocumentMerger.merge(bookB, bookA)
//    result \ "commonField" \ "value" shouldEqual JString("New Field!")
//    result \ "commonField" \ "source" shouldEqual bookB \ "source"
//    reverseResult \ "commonField" shouldEqual JString("New Field!")
//  }
//
//  it should "refuse to combine two book documents with different classifications" in {
//    val bookA = sampleBook(
//      ("field" -> "Field") ~
//      ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~
//      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
//    )
//    val bookB = sampleBook(
//      ("field" -> "A different thing") ~
//      ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "a different id"))) ~
//      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
//    )
//    intercept[DifferentClassificationException] {
//      DocumentMerger.merge(bookA, bookB)
//    }
//    intercept[DifferentClassificationException] {
//      DocumentMerger.merge(bookB, bookA)
//    }
//  }
//
//  it should "not replace old data with new data, on two book documents, if it is from a less trusted source" in {
//    val bookA = sampleBook(
//      ("trust" -> "Trusted Field") ~
//      ("source" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.minusMinutes(1)))
//    )
//    val bookB = sampleBook(
//      ("trust" -> "Less Trusted Field") ~
//      ("source" -> ("role" -> "publisher_ftp") ~ ("deliveredAt" -> DateTime.now.plusMinutes(1)))
//    )
//    val result = DocumentMerger.merge(bookA, bookB)
//    val reverseResult = DocumentMerger.merge(bookB, bookA)
//    result \ "trust" shouldEqual JString("Trusted Field")
//    reverseResult \ "trust" \ "value" shouldEqual JString("Trusted Field")
//    reverseResult \ "trust" \ "source" shouldEqual bookA \ "source"
//  }
//
//  it should "replace old data with new data, on two book documents, if it is from the same trusted source" in {
//    val bookA = sampleBook(
//      ("data" -> "A value") ~
//      ("source" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.minusMinutes(1)))
//    )
//    val bookB = sampleBook(
//      ("data" -> "B value") ~
//      ("source" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.plusMinutes(1)))
//    )
//    val result = DocumentMerger.merge(bookA, bookB)
//    val reverseResult = DocumentMerger.merge(bookB, bookA)
//    result \ "data" \ "value" shouldEqual JString("B value")
//    result \ "data" \ "source" shouldEqual bookB \ "source"
//    reverseResult \ "data" shouldEqual JString("B value")
//  }
//
//  it should "add sub-objects on two book documents" in {
//    val bookA = sampleBook("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
//    val bookB = sampleBook(
//      ("things" -> List(("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~ ("data" -> "test"))) ~
//      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
//    )
//    val result = DocumentMerger.merge(bookA, bookB)
//    val reverseResult = DocumentMerger.merge(bookB, bookA)
//    result \ "things" \ "value" \ "data" shouldEqual JString("test")
//    result \ "things" \ "source" shouldEqual bookB \ "source"
//    reverseResult \ "things" shouldEqual bookB \ "things"
//  }
//
//  it should "merge two different keys with appropriate classifications" in {
//    val bookA =
//      ("source" -> ("a-ness" ->
//        ("system" -> ("name" -> "marvin/design_docs") ~ ("version" -> "1.0.0")) ~
//        ("role" -> "publisher_ftp") ~
//        ("username" -> "jp-publishing") ~
//        ("deliveredAt" -> DateTime.now.minusMinutes(1)) ~
//        ("processedAt" -> DateTime.now.minusMinutes(1)))
//      ) ~
//      ("a-ness" ->
//        ("classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))) ~
//        ("data" -> "Item A")
//      )
//    val bookB =
//      ("source" -> ("b-ness" ->
//        ("system" -> ("name" -> "marvin/design_docs") ~ ("version" -> "1.0.0")) ~
//        ("role" -> "publisher_ftp") ~
//        ("username" -> "jp-publishing") ~
//        ("deliveredAt" -> DateTime.now.plusMinutes(1)) ~
//        ("processedAt" -> DateTime.now.plusMinutes(1)))
//      ) ~
//      ("b-ness" ->
//        ("classification" -> List(("realm" -> "type") ~ ("id" -> "b-ness"))) ~
//        ("data" -> "Item B")
//      )
//    val result = DocumentMerger.merge(bookA, bookB)
//    val reverseResult = DocumentMerger.merge(bookB, bookA)
//    result \ "a-ness" \ "data" shouldEqual JString("Item A")
//    result \ "b-ness" \ "value" \ "data" shouldEqual JString("Item B")
//    result \ "b-ness" \ "source" shouldEqual bookB \ "source"
//    reverseResult \ "a-ness" \ "value" \ "data" shouldEqual JString("Item A")
//    reverseResult \ "a-ness" \ "source" shouldEqual bookA \ "source"
//    reverseResult \ "b-ness" \ "data" shouldEqual JString("Item B")
//  }
//
//  it should "deep merge the same sub-objects on two book documents with different classifications" in {
//    val bookA = sampleBook(
//      "things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))) ~ ("data" -> "Item A"))
//    )
//    val bookB = sampleBook(
//      "things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "b-ness"))) ~ ("data" -> "Item B"))
//    )
//    val result = DocumentMerger.merge(bookA, bookB)
//    val reverseResult = DocumentMerger.merge(bookB, bookA)
//    Seq(result, reverseResult).foreach { doc =>
//      (doc \ "things").children.size shouldEqual 2
//    }
//    (result \ "things")(0) shouldEqual (bookA \ "things")(0)
//    (result \ "things")(1) \ "value" shouldEqual (bookB \ "things")(0)
//    (result \ "things")(1) \ "source" shouldEqual bookB \ "source"
//    (reverseResult \ "things")(0) shouldEqual (bookB \ "things")(0)
//    (reverseResult \ "things")(1) \ "value" shouldEqual (bookA \ "things")(0)
//    (reverseResult \ "things")(1) \ "source" shouldEqual bookA \ "source"
//  }
//
//  it should "deep merge different sub-objects on two book documents with different classifications" in {
//    val bookA = sampleBook(
//      "things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))) ~ ("data" -> "Item A"))
//    )
//    val bookB = sampleBook(
//      "thongs" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "b-ness"))) ~ ("data" -> "Item B"))
//    )
//    val result = DocumentMerger.merge(bookA, bookB)
//    val reverseResult = DocumentMerger.merge(bookB, bookA)
//    (result \ "things").children.size shouldEqual 1
//    result \ "things" shouldEqual bookA \ "things"
//    result \ "thongs" \ "value" shouldEqual bookB \ "thongs"
//    result \ "thongs" \ "source" shouldEqual bookB \ "source"
//    (reverseResult \ "thongs").children.size shouldEqual 1
//    reverseResult \ "thongs" shouldEqual bookB \ "thongs"
//    reverseResult \ "things" \ "value" shouldEqual bookA \ "things"
//    reverseResult \ "things" \ "source" shouldEqual bookA \ "source"
//  }
//
//  it should "replace an older sub-object with a newer one, on two book documents, if they have the same classification" in {
//    val bookA = sampleBook(
//      ("things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "p-ness"))) ~ ("data" -> "Older"))) ~
//      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
//    )
//    val bookB = sampleBook(
//      ("things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "p-ness"))) ~ ("data" -> "Newer"))) ~
//      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
//    )
//    val result = DocumentMerger.merge(bookA, bookB)
//    val reverseResult = DocumentMerger.merge(bookB, bookA)
//    Seq(result, reverseResult).foreach { doc =>
//      (doc \ "things").children.size shouldEqual 1
//    }
//    (result \ "things")(0) \ "classification" shouldEqual (bookB \ "things")(0) \ "classification"
//    (result \ "things")(0) \ "data" \ "value" shouldEqual (bookB \ "things")(0) \ "data"
//    (result \ "things")(0) \ "data" \ "source" shouldEqual (bookB \ "source")
//    (reverseResult \ "things")(0) shouldEqual (bookB \ "things")(0)
//  }
//
//  it should "merge classified arrays with duplicate keys" in {
//    val aNessClassification: JField = "classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))
//    val pNessClassification: JField = "classification" -> List(("realm" -> "type") ~ ("id" -> "p-ness"))
//    val bookA = sampleBook(
//      ("things" -> List(
//        aNessClassification ~ ("a" -> "Older"),
//        aNessClassification ~ ("b" -> "Older")
//      )) ~
//      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
//    )
//    val bookB = sampleBook(
//      ("things" -> List(
//        pNessClassification ~ ("c" -> "Newer"),
//        pNessClassification ~ ("d" -> "Newer")
//      )) ~
//      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
//    )
//    val aSource: JField = "source" -> bookA \ "source"
//    val bSource: JField = "source" -> bookB \ "source"
//    val result = DocumentMerger.merge(bookA, bookB)
//    val reverseResult = DocumentMerger.merge(bookB, bookA)
//    Seq(result, reverseResult).foreach { doc =>
//      (doc \ "things").children.size shouldEqual 2
//    }
//    println("res = " + pretty(render(result)))
//    println("rev = " + pretty(render(reverseResult)))
//    (result \ "things")(0) shouldEqual (aNessClassification ~ ("a" -> "Older") ~ ("b" -> "Older"))
//    (result \ "things")(1) shouldEqual (pNessClassification ~ ("c" -> "Newer") ~ ("d" -> "Newer") ~ bSource)
//    (reverseResult \ "things").children should contain (aNessClassification ~ ("a" -> "Older") ~ ("b" -> "Older") ~ aSource)
//    (reverseResult \ "things").children should contain (pNessClassification ~ ("c" -> "Newer") ~ ("d" -> "Newer"))
//  }
//
//  it should "merge classified arrays with the same classification, with multiple keys along with their source" in {
//    val aNessClassification: JField = "classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))
//    val bookA = sampleBook(
//      ("things" -> List(aNessClassification ~ ("a" -> "Older") ~ ("b" -> "Older"))) ~
//      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
//    )
//    val bookB = sampleBook(
//      ("things" -> List(aNessClassification ~ ("b" -> "Newer") ~ ("c" -> "Newer"))) ~
//      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
//    )
//    val result = DocumentMerger.merge(bookA, bookB)
//    val reverseResult = DocumentMerger.merge(bookB, bookA)
//    Seq(result, reverseResult).foreach { doc =>
//      (doc \ "things").children.size == 1)
//      (doc \ "things")(0) \ "a" == JString("Older"))
//      (doc \ "things")(0) \ "b" == JString("Newer"))
//      (doc \ "things")(0) \ "c" == JString("Newer"))
//    }
//    result \ "things" \ "source" \ "c" == bookB \ "source" \ "$remaining")
//    result \ "things" \ "source" \ "b" == bookB \ "source" \ "$remaining")
//    reverseResult \ "things" \ "source" \ "a" == bookA \ "source" \ "$remaining")
//  }
//
//  it should "replace an older object with a newer one, and merge the unique ones" in {
//    val bookA = sampleBook(
//      ("a-cool" -> List("cool" -> "a","sweet" -> "2")) ~
//      ("cool" -> List("t1" -> 1, "t2" -> 2)) ~
//      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
//    )
//    val bookB = sampleBook(
//      ("b-cool" -> List("cool" -> "b", "sweet" -> "2")) ~
//      ("cool" -> List("t3" -> 3, "t4" -> 4)) ~
//      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
//    )
//    val result = DocumentMerger.merge(bookA, bookB)
//    val reverseResult = DocumentMerger.merge(bookB, bookA)
//    Seq(result, reverseResult).foreach { doc =>
//      (doc \ "cool").children == List[JValue]("t3" -> 3, "t4" -> 4))
//      (doc \ "a-cool").children == List[JValue]("cool" -> "a", "sweet" -> "2"))
//      (doc \ "b-cool").children == List[JValue]("cool" -> "b", "sweet" -> "2"))
//    }
//    result \ "source" \ "cool" != JNothing)
//    result \ "source" \ "b-cool" != JNothing)
//    reverseResult \ "source" \ "cool" == JNothing)
//    reverseResult \ "source" \ "a-cool" != JNothing)
//  }
//
//  it should "correctly merge object fields and sub keys should be treated as if they were parent keys" in {
//    val bookA = sampleBook(
//      ("a" -> ("b" -> "1ab")) ~
//      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1))))
//    )
//    val bookB = sampleBook(
//      ("a" -> ("c" -> "2ac")) ~
//      ("source" -> ("deliveredAt" -> DateTime.now)))
//    )
//    val bookC = sampleBook(
//      ("a" -> ("b" -> "3ab")) ~
//      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1))))
//    )
//    val resultA = DocumentMerger.merge(bookA, bookB)
//    val reverseResultA = DocumentMerger.merge(bookB, bookA)
//    val resultB = DocumentMerger.merge(bookA, bookC)
//    val reverseResultB = DocumentMerger.merge(bookC, bookA)
//    Seq(resultA, reverseResultA).foreach { doc =>
//      doc \ "a" == (("b" -> "1ab") ~ ("c" -> "2ac")))
//    }
//    Seq(resultB, reverseResultB).foreach { doc =>
//      doc \ "a" == JObject(List[JField]("b" -> "3ab"): _*))
//    }
//  }
}
