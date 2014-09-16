package com.blinkbox.books.marvin.magrathea.message

import com.blinkbox.books.json.DefaultFormats
import org.joda.time.DateTime
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.annotation.tailrec

object DocumentMerger {
  case class DifferentClassificationException(c1: JValue, c2: JValue) extends RuntimeException(
    s"Cannot merge documents with different classifications:\n- ${compact(render(c1))}\n- ${compact(render(c2))}")

  implicit val json4sJacksonFormats = DefaultFormats
  private val StaticKeys = Seq("source", "classification", "$schema", "_id", "_rev")
  private val AuthorityRoles = Seq("publisher_ftp", "content_manager")
  // We don't compare these fields, as they're not source data or are important to the comparison
  private val nonStaticKeys: String => Boolean = !StaticKeys.contains(_)

  def merge(docA: JValue, docB: JValue): JValue = {
    if ((docA \ "classification") != (docB \ "classification"))
      throw DifferentClassificationException(docA \ "classification", docB \ "classification")

    // "b" will be emitted, so update it with data from "a". If an element of "a"
    // is newer, ensure there's an object representing the times things were updated.
    docA.asInstanceOf[JObject].values.keys.filter(nonStaticKeys).foldLeft(docB) { (acc, field) =>
      deepMergeField(field, docA, docB, acc).getOrElse(replaceField(field, docA, docB).getOrElse(acc))
    }
  }

  private def replaceField(field: String, docA: JValue, docB: JValue): Option[JValue] = {
    prepareReplaceField(field, docA, docB).map { case (docField, docSourceField) =>
      docB merge docField merge docSourceField
    }
  }

  private def deepMergeField(field: String, docA: JValue, docB: JValue, acc: JValue): Option[JValue] = {
    prepareClassifiedArrays(field, docA, docB).map { case (cA, cB) =>
      var merged = merge(cA, cB)
      // move sources out of the hash
      val sources: JValue = merged \ "source"
      merged = merged.removeField(_._1 == "source")
      // turn the merged object into an array
      val asArray = merged.asInstanceOf[JObject].values.keys.map { subKey =>
        val newSubKey: JValue = subKey -> ("source" -> sources \ subKey)
        merged = merged merge newSubKey
        merged \ subKey
      }
      // save the array back into the result
      val newKey: JValue = field -> asArray
      acc.removeField(_._1 == field) merge newKey
    }
  }

  private def prepareReplaceField(field: String, docA: JValue, docB: JValue): Option[(JValue, JValue)] = {
    val docField: JValue = field -> docA \ field
    val docSourceField: JValue = "source" -> (field -> docA \ "source" \ field)
    if ((docB \ field) == JNothing) Some((docField, docSourceField))
    else for {
      sourceA <- (docA \ "source" \ field).toOption orElse (docA \ "source" \ "$remaining").toOption
      sourceB <- (docB \ "source" \ field).toOption orElse (docB \ "source" \ "$remaining").toOption
      deliveredA = (sourceA \ "deliveredAt").extract[DateTime]
      deliveredB = (sourceB \ "deliveredAt").extract[DateTime]
      roleA = (sourceA \ "role").extract[String]
      roleB = (sourceB \ "role").extract[String]
      aIsNewerThanB = deliveredA.isAfter(deliveredB)
      aIsAuthorisedToReplaceB = AuthorityRoles.indexOf(roleA) >= AuthorityRoles.indexOf(roleB)
      aBias = AuthorityRoles.indexOf(roleA) - AuthorityRoles.indexOf(roleB)
      if (aIsNewerThanB && aIsAuthorisedToReplaceB) || aBias > 0
    } yield (docField, docSourceField)
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
