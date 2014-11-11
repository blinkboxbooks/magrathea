package com.blinkbox.books.marvin.magrathea

import java.util.UUID

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

trait JsonDoc {
  def id: UUID
  def schema: String
  def toJson: JValue
}

case class History(id: UUID, schema: String, classification: JValue, doc: JValue, source: JValue) extends JsonDoc {
  lazy val toJson: JValue = {
    val schemaField: JValue = "$schema" -> schema
    val classificationField: JValue = "classification" -> classification
    val sourceField: JValue = "source" -> source
    schemaField merge classificationField merge doc merge sourceField
  }
}

case class Latest(id: UUID, schema: String, classification: JValue, doc: JValue, source: JValue) extends JsonDoc {
  lazy val toJson: JValue = {
    val schemaField: JValue = "$schema" -> schema
    val classificationField: JValue = "classification" -> classification
    val sourceField: JValue = "source" -> source
    schemaField merge classificationField merge doc merge sourceField
  }
}
