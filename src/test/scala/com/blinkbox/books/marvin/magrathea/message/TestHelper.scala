package com.blinkbox.books.marvin.magrathea.message

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.JsonAST.{JNothing, JString, JValue}
import org.json4s.JsonDSL._

import scala.language.implicitConversions
import scala.util.Random

trait TestHelper {
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

  def lookupKeyMatch(extraContent: JValue = JNothing): JValue = {
    ("id" -> "7e93a26396bde6994ccefaf3da003659") ~
    ("key" -> List("whatever-does-not-matter")) ~
    ("value" -> ("_id" -> "7e93a26396bde6994ccefaf3da003659") ~ ("_rev" -> "1-da5a08470ffe6bca22174a02f5fd5714"))
  }
}
