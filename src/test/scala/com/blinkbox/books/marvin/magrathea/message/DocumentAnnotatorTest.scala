package com.blinkbox.books.marvin.magrathea.message

import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.marvin.magrathea.message.DocumentAnnotator._
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, Matchers}

import scala.language.{implicitConversions, postfixOps}

@RunWith(classOf[JUnitRunner])
class DocumentAnnotatorTest extends FlatSpecLike with Matchers with TestHelper {
  "The document annotator" should "refuse to annotate a document without source" in {
    val doc = sampleBook().removeDirectField("source")
    intercept[MissingSourceException] {
      DocumentAnnotator.annotate(doc)
    }
  }

  it should "not annotate $schema and classification" in {
    val doc = sampleBook()
    val res = DocumentAnnotator.annotate(doc)
    res \ "$schema" shouldEqual doc \ "$schema"
    res \ "classification" shouldEqual doc \ "classification"
  }

  it should "annotate fields with primitive values" in {
    val doc = sampleBook(("fieldA" -> "Value A") ~ ("fieldB" -> "Value B"))
    val res = DocumentAnnotator.annotate(doc)
    res \ "fieldA" \ "value" shouldEqual JString("Value A")
    res \ "fieldA" \ "source" shouldEqual JString((doc \ "source").sha1)
    res \ "fieldB" \ "value" shouldEqual JString("Value B")
    res \ "fieldB" \ "source" shouldEqual JString((doc \ "source").sha1)
  }

  it should "annotate object's fields separately" in {
    val doc = sampleBook("obj" -> (("fieldA" -> "Value A") ~ ("fieldB" -> "Value B")))
    val res = DocumentAnnotator.annotate(doc)
    res \ "obj" \ "value" shouldEqual JNothing
    res \ "obj" \ "source" shouldEqual JNothing
    res \ "obj" \ "fieldA" \ "value" shouldEqual JString("Value A")
    res \ "obj" \ "fieldA" \ "source" shouldEqual JString((doc \ "source").sha1)
    res \ "obj" \ "fieldB" \ "value" shouldEqual JString("Value B")
    res \ "obj" \ "fieldB" \ "source" shouldEqual JString((doc \ "source").sha1)
  }

  it should "annotate a classified array's fields separately" in {
    val itemA: JValue = ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "a id"))) ~ ("fieldA" -> "Value A")
    val itemB: JValue = ("classification" -> List(("realm" -> "b realm") ~ ("id" -> "b id"))) ~ ("fieldB" -> "Value B")
    val doc = sampleBook("arr" -> List(itemA, itemB))
    val res = DocumentAnnotator.annotate(doc)
    (res \ "arr").children.size shouldEqual 2
    (res \ "arr").children should contain (("value" -> itemA) ~ ("source" -> (doc \ "source").sha1))
    (res \ "arr").children should contain (("value" -> itemB) ~ ("source" -> (doc \ "source").sha1))
  }

  it should "annotate a non-classified array with a single field as a primitive value" in {
    val doc = sampleBook("arr" -> List("fieldA" -> "Value A"))
    val res = DocumentAnnotator.annotate(doc)
    (res \ "arr" \ "value").children shouldEqual List[JValue]("fieldA" -> "Value A")
    res \ "arr" \ "source" shouldEqual JString((doc \ "source").sha1)
  }

  it should "annotate a non-classified array with multiple fields as a primitive value" in {
    val doc = sampleBook("arr" -> List("fieldA" -> "Value A", "fieldB" -> "Value B"))
    val res = DocumentAnnotator.annotate(doc)
    (res \ "arr" \ "value").children shouldEqual List[JValue]("fieldA" -> "Value A", "fieldB" -> "Value B")
    res \ "arr" \ "source" shouldEqual JString((doc \ "source").sha1)
  }

  it should "not annotate an annotated document" in {
    val cItemA: JValue = ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "a id"))) ~ ("fieldA" -> "Value A")
    val cItemB: JValue = ("classification" -> List(("realm" -> "b realm") ~ ("id" -> "b id"))) ~ ("fieldB" -> "Value B")
    val doc = sampleBook(
      ("fieldA" -> "Value A") ~
      ("fieldB" -> "Value B") ~
      ("obj" -> (("aField" -> "a") ~ ("bField" -> "b"))) ~
      ("cArray" -> List(cItemA, cItemB)) ~
      ("array" -> List("aItem" -> "a", "bItem" -> "b"))
    )
    val res = DocumentAnnotator.annotate(doc)
    res \ "fieldA" \ "value" shouldEqual JString("Value A")
    res \ "fieldA" \ "source" shouldEqual JString((doc \ "source").sha1)
    res \ "fieldB" \ "value" shouldEqual JString("Value B")
    res \ "fieldB" \ "source" shouldEqual JString((doc \ "source").sha1)
    res \ "obj" \ "value" shouldEqual JNothing
    res \ "obj" \ "source" shouldEqual JNothing
    res \ "obj" \ "aField" \ "value" shouldEqual JString("a")
    res \ "obj" \ "aField" \ "source" shouldEqual JString((doc \ "source").sha1)
    res \ "obj" \ "bField" \ "value" shouldEqual JString("b")
    res \ "obj" \ "bField" \ "source" shouldEqual JString((doc \ "source").sha1)
    (res \ "cArray").children.size shouldEqual 2
    (res \ "cArray").children should contain (("value" -> cItemA) ~ ("source" -> (doc \ "source").sha1))
    (res \ "cArray").children should contain (("value" -> cItemB) ~ ("source" -> (doc \ "source").sha1))
    (res \ "array" \ "value").children shouldEqual List[JValue]("aItem" -> "a", "bItem" -> "b")
    res \ "array" \ "source" shouldEqual JString((doc \ "source").sha1)
    DocumentAnnotator.annotate(res) shouldEqual res
  }

  it should "deannotate an annotated document with primitive values" in {
    val doc = annotatedSampleBook(("fieldA" -> "Value A") ~ ("fieldB" -> "Value B"))
    val res = DocumentAnnotator.deAnnotate(doc)
    res \ "fieldA" shouldEqual JString("Value A")
    res \ "fieldB" shouldEqual JString("Value B")
  }

  it should "deannotate an annotated document with an object" in {
    val doc = annotatedSampleBook("obj" -> (("fieldA" -> "Value A") ~ ("fieldB" -> "Value B")))
    val res = DocumentAnnotator.deAnnotate(doc)
    res \ "obj" \ "fieldA" shouldEqual JString("Value A")
    res \ "obj" \ "fieldB" shouldEqual JString("Value B")
  }

  it should "deannotate an annotated document with a classified array" in {
    val itemA: JValue = ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "a id"))) ~ ("fieldA" -> "Value A")
    val itemB: JValue = ("classification" -> List(("realm" -> "b realm") ~ ("id" -> "b id"))) ~ ("fieldB" -> "Value B")
    val doc = annotatedSampleBook("arr" -> List(itemA, itemB))
    val res = DocumentAnnotator.deAnnotate(doc)
    (res \ "arr").children.size shouldEqual 2
    (res \ "arr").children should contain (itemA)
    (res \ "arr").children should contain (itemB)
  }

  it should "deannotate an annotated document with a non-classified array with a single field as a primitive value" in {
    val doc = annotatedSampleBook("arr" -> List("fieldA" -> "Value A"))
    val res = DocumentAnnotator.deAnnotate(doc)
    (res \ "arr").children shouldEqual List[JValue]("fieldA" -> "Value A")
  }

  it should "deannotate an annotated document with a non-classified array with multiple fields as a primitive value" in {
    val doc = annotatedSampleBook("arr" -> List("fieldA" -> "Value A", "fieldB" -> "Value B"))
    val res = DocumentAnnotator.deAnnotate(doc)
    (res \ "arr").children shouldEqual List[JValue]("fieldA" -> "Value A", "fieldB" -> "Value B")
  }

  it should "not deannotate a deannotated document" in {
    val cItemA: JValue = ("classification" -> List(("realm" -> "a realm") ~ ("id" -> "a id"))) ~ ("fieldA" -> "Value A")
    val cItemB: JValue = ("classification" -> List(("realm" -> "b realm") ~ ("id" -> "b id"))) ~ ("fieldB" -> "Value B")
    val doc = annotatedSampleBook(
      ("fieldA" -> "Value A") ~
      ("fieldB" -> "Value B") ~
      ("obj" -> (("aField" -> "a") ~ ("bField" -> "b"))) ~
      ("cArray" -> List(cItemA, cItemB)) ~
      ("array" -> List("aItem" -> "a", "bItem" -> "b"))
    )
    val res = DocumentAnnotator.deAnnotate(doc)
    res \ "fieldA" shouldEqual JString("Value A")
    res \ "fieldB" shouldEqual JString("Value B")
    res \ "obj" \ "aField" shouldEqual JString("a")
    res \ "obj" \ "bField" shouldEqual JString("b")
    (res \ "cArray").children.size shouldEqual 2
    (res \ "cArray").children should contain (cItemA)
    (res \ "cArray").children should contain (cItemB)
    (res \ "array").children shouldEqual List[JValue]("aItem" -> "a", "bItem" -> "b")
    DocumentAnnotator.deAnnotate(res) shouldEqual res
  }
}
