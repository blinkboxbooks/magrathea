package com.blinkbox.books.marvin.magrathea.message

import org.json4s.JsonAST.{JNothing, JValue}
import org.json4s.JsonDSL._

import scala.util.Random

object Helper {
  def getRandomIsbn(num: Int = Random.nextInt(1000000)) = s"9780007${"%06d".format(num)}"

  def sampleBook(extraContent: JValue = JNothing): JValue = {
    val doc: JValue =
      ("$schema" -> "ingestion.book.metadata.v2") ~
      ("classification" -> "something") ~
      ("source" ->
        ("system" ->
          ("name" -> "marvin/design_docs") ~
          ("version" -> "1.0.0")
        ) ~
        ("role" -> "publisher_ftp") ~
        ("username" -> "jp-publishing"))
    doc merge extraContent
  }
}
