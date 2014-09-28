package com.blinkbox.books.marvin.magrathea.message

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.Json4sExtensions._
import com.blinkbox.books.marvin.magrathea.message.DocumentMerger._
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, Matchers}
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

  "The document merger" should "refuse to combine two book documents with different schema" in {
    val bookA = sampleBook(
      ("$schema" -> "ingestion.book.metadata.v2") ~
      ("field" -> "Field") ~
      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    )
    val bookB = sampleBook(
      ("$schema" -> "ingestion.contributor.metadata.v2") ~
      ("field" -> "A different thing") ~
      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    intercept[DifferentSchemaException] {
      DocumentMerger.merge(bookA, bookB)
    }
    intercept[DifferentSchemaException] {
      DocumentMerger.merge(bookB, bookA)
    }
  }

  it should "refuse to combine two book documents with different classifications" in {
    val bookA = sampleBook(
      ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~
      ("field" -> "Field") ~
      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    )
    val bookB = sampleBook(
      ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "a different id"))) ~
      ("field" -> "A different thing") ~
      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    intercept[DifferentClassificationException] {
      DocumentMerger.merge(bookA, bookB)
    }
    intercept[DifferentClassificationException] {
      DocumentMerger.merge(bookB, bookA)
    }
  }

  it should "refuse to combine two book documents without any sources" in {
    val bookA = sampleBook("field" -> "Field").removeDirectField("source")
    val bookB = sampleBook("field" -> "A different thing").removeDirectField("source")
    intercept[MissingSourceException] {
      DocumentMerger.merge(bookA, bookB)
    }
    intercept[MissingSourceException] {
      DocumentMerger.merge(bookB, bookA)
    }
  }

  it should "combine two book documents with two unique keys" in {
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
    Seq(result, reverseResult).foreach { doc =>
      doc \ "fieldA" \ "value" shouldEqual JString("Value A")
      doc \ "fieldA" \ "source" shouldEqual JString((bookA \ "source").sha1)
      doc \ "fieldB" \ "value" shouldEqual JString("Value B")
      doc \ "fieldB" \ "source" shouldEqual JString((bookB \ "source").sha1)
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual bookA \ "source"
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual bookB \ "source"
    }
  }

  it should "combine two book documents so that more recent information is emitted" in {
    val bookA = sampleBook(
      ("commonField" -> "Old Field") ~
      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    )
    val bookB = sampleBook(
      ("commonField" -> "New Field!") ~
      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      doc \ "commonField" \ "value" shouldEqual JString("New Field!")
      doc \ "commonField" \ "source" shouldEqual JString((bookB \ "source").sha1)
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual JNothing
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual bookB \ "source"
    }
  }

  it should "not replace old data with new data, on two book documents, if it is from a less trusted source" in {
    val bookA = sampleBook(
      ("trust" -> "Trusted Field") ~
      ("source" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    )
    val bookB = sampleBook(
      ("trust" -> "Less Trusted Field") ~
      ("source" -> ("role" -> "publisher_ftp") ~ ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      doc \ "trust" \ "value" shouldEqual JString("Trusted Field")
      doc \ "trust" \ "source" shouldEqual JString((bookA \ "source").sha1)
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual bookA \ "source"
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual JNothing
    }
  }

  it should "replace old data with new data, on two book documents, if it is from the same trusted source" in {
    val bookA = sampleBook(
      ("data" -> "A value") ~
      ("source" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    )
    val bookB = sampleBook(
      ("data" -> "B value") ~
      ("source" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      doc \ "data" \ "value" shouldEqual JString("B value")
      doc \ "data" \ "source" shouldEqual JString((bookB \ "source").sha1)
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual JNothing
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual bookB \ "source"
    }
  }

  it should "add sub-objects on two book documents" in {
    val bookA = sampleBook("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    val bookB = sampleBook(
      ("things" -> List("data" -> "test")) ~
      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      doc \ "things" \ "value" \ "data" shouldEqual JString("test")
      doc \ "things" \ "source" shouldEqual JString((bookB \ "source").sha1)
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual JNothing
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual bookB \ "source"
    }
  }

  it should "add classified sub-objects on two book documents" in {
    val bookA = sampleBook("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    val bookB = sampleBook(
      ("things" -> List(("classification" -> List(("realm" -> "a realm") ~ ("id" -> "an id"))) ~ ("data" -> "test"))) ~
      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      doc \ "things" \ "value" \ "data" shouldEqual JString("test")
      doc \ "things" \ "source" shouldEqual JString((bookB \ "source").sha1)
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual JNothing
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual bookB \ "source"
    }
  }

  it should "merge two different keys with appropriate classifications" in {
    val bookA = sampleBook(
      ("a-ness" -> List(
        ("classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))) ~ ("data" -> "Item A")
      )) ~
      ("source" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    )
    val bookB = sampleBook(
      ("b-ness" -> List(
        ("classification" -> List(("realm" -> "type") ~ ("id" -> "b-ness"))) ~ ("data" -> "Item B")
      )) ~
      ("source" -> ("role" -> "content_manager") ~ ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      doc \ "a-ness" \ "value" \ "data" shouldEqual JString("Item A")
      doc \ "a-ness" \ "source" shouldEqual JString((bookA \ "source").sha1)
      doc \ "b-ness" \ "value" \ "data" shouldEqual JString("Item B")
      doc \ "b-ness" \ "source" shouldEqual JString((bookB \ "source").sha1)
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual bookA \ "source"
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual bookB \ "source"
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
      (doc \ "things").children.size shouldEqual 2
      (doc \ "things").children should contain (("value" -> (bookA \ "things")(0)) ~ ("source" -> (bookA \ "source").sha1))
      (doc \ "things").children should contain (("value" -> (bookB \ "things")(0)) ~ ("source" -> (bookB \ "source").sha1))
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual bookA \ "source"
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual bookB \ "source"
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
      doc \ "things" \ "value" \ "data" shouldEqual JString("Item A")
      doc \ "things" \ "source" shouldEqual JString((bookA \ "source").sha1)
      doc \ "thongs" \ "value" \ "data" shouldEqual JString("Item B")
      doc \ "thongs" \ "source" shouldEqual JString((bookB \ "source").sha1)
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual bookA \ "source"
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual bookB \ "source"
    }
  }

  it should "replace an older sub-object with a newer one, on two book documents, if they have the same classification" in {
    val bookA = sampleBook(
      ("things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "p-ness"))) ~ ("data" -> "Older"))) ~
      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    )
    val bookB = sampleBook(
      ("things" -> List(("classification" -> List(("realm" -> "type") ~ ("id" -> "p-ness"))) ~ ("data" -> "Newer"))) ~
      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      doc \ "things" \ "value" \ "data" shouldEqual JString("Newer")
      doc \ "things" \ "source" shouldEqual JString((bookB \ "source").sha1)
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual JNothing
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual bookB \ "source"
    }
  }

  it should "merge classified arrays with duplicate keys" in {
    val aNessClassification: JField = "classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))
    val pNessClassification: JField = "classification" -> List(("realm" -> "type") ~ ("id" -> "p-ness"))
    val bookA = sampleBook(
      ("dlist" -> List(
        aNessClassification ~ ("a" -> "Older"),
        aNessClassification ~ ("b" -> "Older")
      )) ~
      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    )
    val bookB = sampleBook(
      ("dlist" -> List(
        pNessClassification ~ ("c" -> "Newer"),
        pNessClassification ~ ("d" -> "Newer")
      )) ~
      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      (doc \ "dlist").children.size shouldEqual 2
      (doc \ "dlist").children should contain (("value" ->
        (aNessClassification ~ ("a" -> "Older") ~ ("b" -> "Older"))) ~ ("source" -> (bookA \ "source").sha1))
      (doc \ "dlist").children should contain (("value" ->
        (pNessClassification ~ ("c" -> "Newer") ~ ("d" -> "Newer"))) ~ ("source" -> (bookB \ "source").sha1))
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual bookA \ "source"
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual bookB \ "source"
    }
  }

  ignore should "merge classified arrays with the same classification, with multiple keys along with their source" in {
    val aNessClassification: JField = "classification" -> List(("realm" -> "type") ~ ("id" -> "a-ness"))
    val bookA = sampleBook(
      ("things" -> List(aNessClassification ~ ("a" -> "Older") ~ ("b" -> "Older"))) ~
      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    )
    val bookB = sampleBook(
      ("things" -> List(aNessClassification ~ ("b" -> "Newer") ~ ("c" -> "Newer"))) ~
      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      // merging classified arrays is not supported ATM
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual bookA \ "source"
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual bookB \ "source"
    }
  }

  it should "replace an older object with a newer one, and merge the unique ones" in {
    val bookA = sampleBook(
      ("a-cool" -> List("cool" -> "a", "sweet" -> "2")) ~
      ("cool" -> List("t1" -> 1, "t2" -> 2)) ~
      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    )
    val bookB = sampleBook(
      ("b-cool" -> List("cool" -> "b", "sweet" -> "2")) ~
      ("cool" -> List("t3" -> 3, "t4" -> 4)) ~
      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    val result = DocumentMerger.merge(bookA, bookB)
    val reverseResult = DocumentMerger.merge(bookB, bookA)
    Seq(result, reverseResult).foreach { doc =>
      (doc \ "cool" \ "value").children shouldEqual List[JValue]("t3" -> 3, "t4" -> 4)
      doc \ "cool" \ "source" shouldEqual JString((bookB \ "source").sha1)
      (doc \ "a-cool" \ "value").children shouldEqual List[JValue]("cool" -> "a", "sweet" -> "2")
      doc \ "a-cool" \ "source" shouldEqual JString((bookA \ "source").sha1)
      (doc \ "b-cool" \ "value").children shouldEqual List[JValue]("cool" -> "b", "sweet" -> "2")
      doc \ "b-cool" \ "source" shouldEqual JString((bookB \ "source").sha1)
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual bookA \ "source"
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual bookB \ "source"
    }
  }

  it should "correctly merge object fields and sub keys should be treated as if they were parent keys" in {
    val bookA = sampleBook(
      ("a" -> ("b" -> "1ab")) ~
      ("source" -> ("deliveredAt" -> DateTime.now.minusMinutes(1)))
    )
    val bookB = sampleBook(
      ("a" -> ("c" -> "2ac")) ~
      ("source" -> ("deliveredAt" -> DateTime.now))
    )
    val bookC = sampleBook(
      ("a" -> ("b" -> "3ab")) ~
      ("source" -> ("deliveredAt" -> DateTime.now.plusMinutes(1)))
    )
    val resultA = DocumentMerger.merge(bookA, bookB)
    val reverseResultA = DocumentMerger.merge(bookB, bookA)
    Seq(resultA, reverseResultA).foreach { doc =>
      doc \ "a" \ "b" \ "value" shouldEqual JString("1ab")
      doc \ "a" \ "b" \ "source" shouldEqual JString((bookA \ "source").sha1)
      doc \ "a" \ "c" \ "value" shouldEqual JString("2ac")
      doc \ "a" \ "c" \ "source" shouldEqual JString((bookB \ "source").sha1)
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual bookA \ "source"
      doc \ "source" \ (bookB \ "source").sha1 shouldEqual bookB \ "source"
    }
    val resultB = DocumentMerger.merge(bookA, bookC)
    val reverseResultB = DocumentMerger.merge(bookC, bookA)
    Seq(resultB, reverseResultB).foreach { doc =>
      doc \ "a" \ "b" \ "value" shouldEqual JString("3ab")
      doc \ "a" \ "b" \ "source" shouldEqual JString((bookC \ "source").sha1)
      doc \ "source" \ (bookA \ "source").sha1 shouldEqual JNothing
      doc \ "source" \ (bookC \ "source").sha1 shouldEqual bookC \ "source"
    }
  }
}
