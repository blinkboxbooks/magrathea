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
    var res = docB
    docA.asInstanceOf[JObject].values.keys.filter(nonStaticKeys).foreach { field =>
      var replaceContents = true
      if (canDeepMergeField(docA, field)) {
        deepMergeField(docA, docB, res, field).foreach { doc =>
          res = doc
          replaceContents = false
        }
      }
      if (canReplaceField(docA, docB, field, replaceContents)) {
        res = replaceField(field, docA, res)
      }
    }
    res
  }

  private def canDeepMergeField(doc: JValue, field: String): Boolean =
    (for {
      key <- (doc \ field).toOption
      isArray = key.isInstanceOf[JArray]
      hasChildren = key.children.size > 0
      hasClassification = (key \ "classification").toOption.isDefined
    } yield isArray && hasChildren && hasClassification) match {
      case Some(value) => value
      case _ => false
    }

  private def canReplaceField(docA: JValue, docB: JValue, field: String, replaceContents: Boolean): Boolean =
    if ((docB \ field) == JNothing) true
    else (for {
      sourceA <- (docA \ "source" \ field).toOption orElse (docA \ "source" \ "$remaining").toOption
      sourceB <- (docB \ "source" \ field).toOption orElse (docB \ "source" \ "$remaining").toOption
      deliveredA = (sourceA \ "deliveredAt").extract[DateTime]
      deliveredB = (sourceB \ "deliveredAt").extract[DateTime]
      roleA = (sourceA \ "role").extract[String]
      roleB = (sourceB \ "role").extract[String]
      aIsNewerThanB = deliveredA.isAfter(deliveredB)
      aIsAuthorisedToReplaceB = AuthorityRoles.indexOf(roleA) >= AuthorityRoles.indexOf(roleB)
      aBias = AuthorityRoles.indexOf(roleA) - AuthorityRoles.indexOf(roleB)
    } yield replaceContents && ((aIsNewerThanB && aIsAuthorisedToReplaceB) || aBias > 0)) match {
      case Some(value) => value
      case _ => false
    }

  private def replaceField(field: String, docA: JValue, docB: JValue): JValue = {
    val fromKey: JValue = field -> docA \ field
    val fromSourceKey: JValue = "source" -> (field -> docA \ "source" \ field)
    docB merge fromKey merge fromSourceKey
  }

  private def deepMergeField(docA: JValue, docB: JValue, current: JValue, field: String): Option[JValue] = {
    val classifiedA = prepareClassifiedArray(docA \ field, docA \ "source" \ "$remaining")
    val classifiedB = prepareClassifiedArray(docB \ field, docB \ "source" \ "$remaining")
    (classifiedA, classifiedB) match {
      case (Some(cA), Some(cB)) =>
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
        Some(current.removeField(_._1 == field) merge newKey)
      case _ =>
        // one of the two arrays isn't a proper classified array,
        // we must just replace the older with the newer
        None
    }
  }

  private def prepareClassifiedArray(array: JValue, parentSource: JValue): Option[JValue] =
    classifiedArray(array.children, parentSource, JNothing).toOption

  @tailrec
  private def classifiedArray(elems: List[JValue], parentSource: JValue, acc: JValue): JValue = elems match {
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
