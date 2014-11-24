package com.blinkbox.books.marvin.magrathea.message

import java.util.UUID

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.marvin.magrathea.JsonDoc
import org.joda.time.DateTime
import org.json4s.JsonAST.JNothing
import org.json4s.{Diff, JValue}

import scala.language.implicitConversions

object DocumentRevisions {
  case class Revision(id: UUID, editor: String, changed: JValue, added: JValue, deleted: JValue, timestamp: DateTime) {
    val hasChanges = changed != JNothing || added != JNothing || deleted != JNothing
  }
  case class PackedRevision(doc: JsonDoc, rev: Revision)

  implicit val json4sJacksonFormats = DefaultFormats

  def fromList(list: List[JsonDoc]): List[Revision] = {
    list.foldLeft(List.empty[PackedRevision]) {
      case (rest @ head :: _, doc) => prependOrElse(doc, diff(head.doc, doc), rest)
      case (Nil, doc) => prependOrElse(doc, diff(JsonDoc.None, doc), Nil)
    }.map(_.rev)
  }

  private def prependOrElse(doc: JsonDoc, revision: Option[Revision], rest: List[PackedRevision]): List[PackedRevision] =
    revision.fold(rest)(rev => PackedRevision(doc, rev) :: rest)

  private def diff(x: JsonDoc, y: JsonDoc): Option[Revision] = {
    val editor = (y.toJson \ "source" \ "system" \ "name").extractOpt[String].getOrElse("unknown")
    val Diff(changed, added, deleted) = stripCommonFields(x.toJson) diff stripCommonFields(y.toJson)
    val timestamp = (y.toJson \ "source" \ "deliveredAt").extract[DateTime]
    if (changed != JNothing || added != JNothing || deleted != JNothing)
      Option(Revision(y.id, editor, changed, added, deleted, timestamp))
    else None
  }

  private def stripCommonFields(doc: JValue): JValue =
    doc.removeDirectField("$schema").removeDirectField("classification").removeDirectField("source")
}
