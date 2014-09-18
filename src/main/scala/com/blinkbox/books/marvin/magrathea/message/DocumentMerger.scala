package com.blinkbox.books.marvin.magrathea.message

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.Json4sExtensions._
import com.typesafe.scalalogging.slf4j.Logger
import org.joda.time.DateTime
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.language.implicitConversions

object DocumentMerger {
  case class DifferentClassificationException(c1: JValue, c2: JValue) extends RuntimeException(
    s"Cannot merge documents with different classifications:\n- ${compact(render(c1))}\n- ${compact(render(c2))}")

  implicit val json4sJacksonFormats = DefaultFormats
  private val StaticKeys = Seq("source", "classification", "$schema", "_id", "_rev")
  private val AuthorityRoles = Seq("publisher_ftp", "content_manager")
  // We don't compare these fields, as they're not source data or are important to the comparison
  private val nonStaticKeys: String => Boolean = !StaticKeys.contains(_)
  private val logger: Logger = Logger(LoggerFactory getLogger getClass.getName)

  def merge(docA: JValue, docB: JValue): JValue = {
    if ((docA \ "classification") != (docB \ "classification"))
      throw DifferentClassificationException(docA \ "classification", docB \ "classification")

    // "b" will be emitted, so update it with data from "a". If an element of "a"
    // is newer, ensure there's an object representing the times things were updated.
    docA.asInstanceOf[JObject].values.keys.filter(nonStaticKeys).foldLeft(docB) { (acc, field) =>
      deepMergeField(field, docA, docB, acc).getOrElse(replaceField(field, docA, docB, acc).getOrElse(acc))
    }
  }

  private def replaceField(field: String, docA: JValue, docB: JValue, acc: JValue): Option[JValue] = {
    prepareReplaceField(field, docA, docB).map { case (docField, docSourceField) =>
      logger.debug(s"Replacing field for '$field'")
      acc.removeDirectField(field) merge docField merge docSourceField
    }
  }

  private def deepMergeField(field: String, docA: JValue, docB: JValue, acc: JValue): Option[JValue] = {
    prepareClassifiedArrays(field, docA, docB).map { case (cA, cB) =>
      logger.debug(s"Deep-merging field for '$field'")
      var merged = merge(cA, cB)
      // move sources out of the hash
      val sources: JValue = merged \ "source"
      merged = merged.removeDirectField("source")
      // turn the merged object into an array
      val asArray = merged.asInstanceOf[JObject].values.keys.map { subKey =>
        val newSubKey: JValue = subKey -> ("source" -> sources \ subKey)
        merged = merged merge newSubKey
        merged \ subKey
      }
      // save the array back into the result
      val newKey: JValue = field -> asArray
      acc.removeDirectField(field) merge newKey
    }
  }

  private def prepareReplaceField(field: String, docA: JValue, docB: JValue): Option[(JValue, JValue)] = {
    val sourceA = ((docA \ "source" \ field).toOption orElse (docA \ "source" \ "$remaining").toOption).getOrElse(
      throw new IllegalArgumentException(s"Cannot find a source for '$field' in ${compact(render(docA))}"))

    val docAField: JValue = field -> docA \ field
    val docASourceField: JValue = "source" -> (field -> sourceA)
    if ((docB \ field) == JNothing) Some((docAField, docASourceField))
    else {
      val sourceB = ((docB \ "source" \ field).toOption orElse (docB \ "source" \ "$remaining").toOption).getOrElse(
        throw new IllegalArgumentException(s"Cannot find a source for '$field' in ${compact(render(docA))}"))
      val deliveredA = (sourceA \ "deliveredAt").extract[DateTime]
      val deliveredB = (sourceB \ "deliveredAt").extract[DateTime]
      val roleA = (sourceA \ "role").extract[String]
      val roleB = (sourceB \ "role").extract[String]
      val aIsNewerThanB = deliveredA.isAfter(deliveredB)
      val aIsAuthorisedToReplaceB = AuthorityRoles.indexOf(roleA) >= AuthorityRoles.indexOf(roleB)
      val aBias = AuthorityRoles.indexOf(roleA) - AuthorityRoles.indexOf(roleB)
      if ((aIsNewerThanB && aIsAuthorisedToReplaceB) || aBias > 0) Some(docAField, docASourceField) else None
    }
  }

  private def prepareClassifiedArrays(field: String, docA: JValue, docB: JValue): Option[(JValue, JValue)] = {
    for {
      key <- (docA \ field).toOption
      isArray = key.isInstanceOf[JArray]
      hasChildren = key.children.size > 0
      hasClassification = (key \ "classification").toOption.isDefined
      if isArray && hasChildren && hasClassification
      cA <- classifiedArray((docA \ field).children, docA \ "source" \ "$remaining").toOption
      cB <- classifiedArray((docB \ field).children, docB \ "source" \ "$remaining").toOption
    } yield (cA, cB)
  }

  @tailrec
  private def classifiedArray(elems: List[JValue], parentSource: JValue, acc: JValue = JNothing): JValue = elems match {
    case item :: items =>
      item \ "classification" match {
        case arr @ JArray(_) =>
          val classification = compact(render(arr))
          val source = (item \ "source").toOption orElse parentSource.toOption
          val classificationField: JValue = classification -> item
          val sourceField: JValue = "source" -> (classification -> source)
          val res = acc merge sourceField merge classificationField
          classifiedArray(items, parentSource, res)
        case _ => JNothing
      }
    case Nil => acc
  }
}
