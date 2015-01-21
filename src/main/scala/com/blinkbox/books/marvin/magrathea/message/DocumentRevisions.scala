package com.blinkbox.books.marvin.magrathea.message

import java.util.UUID

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.marvin.magrathea.{History, JsonDoc}
import org.joda.time.DateTime
import org.json4s.JsonAST.{JNothing, JObject}
import org.json4s.{Diff, JValue}

import scala.language.implicitConversions

case class Revision(id: UUID, editor: String, changed: JValue, added: JValue, deleted: JValue, timestamp: DateTime)

object DocumentRevisions extends (List[History] => List[Revision]) {
  private case class PackedRevision(doc: JsonDoc, rev: Revision)

  private implicit val json4sJacksonFormats = DefaultFormats

  /**
   * Compiles a revision list (diffs) from a list of history documents.
   * This is doing incremental merges for each history document to accurately compare changes, since relying solely on
   * the history documents, we do not have enough information to know whether a field got changed, added or deleted.
   * @param list An incremental list of changes for a specific current document.
   * @return The revision list for the given history documents.
   */
  def apply(list: List[History]): List[Revision] =
    list.foldLeft(List.empty[PackedRevision]) {
      case (Nil, doc) => prependOrElse(JsonDoc.None, doc, Nil)
      case (list @ head :: tail, doc) => prependOrElse(head.doc, doc, list)
    }.map(_.rev)

  /**
   * Calculates the diff between two given documents and if there is one, it adds it to the given list.
   * @param x The first document.
   * @param y The second document.
   * @param list The PackedRevision list.
   * @return The PackedRevision list with or without the new addition.
   */
  private def prependOrElse(x: JsonDoc, y: JsonDoc, list: List[PackedRevision]): List[PackedRevision] = {
    val merged = merge(x, y)
    diff(x, merged).fold(list)(rev => PackedRevision(merged, rev) :: list)
  }

  private def merge(x: JsonDoc, y: JsonDoc): JsonDoc = (x, y) match {
    case (docX: JsonDoc, JsonDoc.None) => docX
    case (JsonDoc.None, docY: JsonDoc) => docY
    case (JsonDoc.None, JsonDoc.None) => JsonDoc.None
    case (docX, docY) => new JsonDoc {
      override val id = docY.id
      override val schema = docY.schema
      override val toJson = DocumentMerger.merge(docX.toJson, docY.toJson)
    }
  }

  /**
   * Checks the diff between two given documents, stripping the irrelevant information.
   * @param x The first document.
   * @param y The second document.
   * @return An optional Revision if there are changes; None if there are no changes.
   */
  private def diff(x: JsonDoc, y: JsonDoc): Option[Revision] = {
    val Diff(changed, added, deleted) = stripIrrelevantData(x) diff stripIrrelevantData(y)
    if (changed != JNothing || added != JNothing || deleted != JNothing)
      Some(Revision(y.id, extractEditor(y), changed, added, deleted, extractTimestamp(y)))
    else None
  }

  private def stripIrrelevantData(doc: JsonDoc): JValue =
    DocumentAnnotator.deAnnotate(doc.toJson
      .removeDirectField("$schema")
      .removeDirectField("classification")
      .removeDirectField("source")
    )

  private def extractEditor(doc: JsonDoc): String = {
    (doc.toJson \ "source" \\ "name" match {
      case JObject(x) => x.last._2
      case value => value
    }).extractOpt[String].getOrElse("unknown")
  }

  private def extractTimestamp(doc: JsonDoc): DateTime =
    (doc.toJson \ "source" \\ "deliveredAt" match {
      case JObject(x) => x.last._2
      case value => value
    }).extract[DateTime]
}
