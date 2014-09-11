package com.blinkbox.books.marvin.magrathea.event

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
  private val noStaticKeys: String => Boolean = key => !StaticKeys.contains(key)

  def merge(docA: JValue, docB: JValue): JValue = {
    if ((docA \ "classification") != (docB \ "classification"))
      throw DifferentClassificationException(docA \ "classification", docB \ "classification")

    // "b" will be emitted, so update it with data from "a". If an element of "a"
    // is newer, ensure there's an object representing the times things were updated.
    var res = docB
    docA.asInstanceOf[JObject].values.keys.filter(noStaticKeys).foreach { key =>
      var replaceContents = true
      for {
        keyA <- (docA \ key).toOption
        isArray = keyA.isInstanceOf[JArray]
        hasChildren = keyA.children.size > 0
        hasClassification = (keyA \ "classification").toOption.isDefined
        if isArray && hasChildren && hasClassification
      } {
        val classifiedA = prepareClassifiedArray(docA \ key, docA \ "source" \ "$remaining")
        val classifiedB = prepareClassifiedArray(docB \ key, docB \ "source" \ "$remaining")
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
            val newKey: JValue = key -> asArray
            res = res.removeField(_._1 == key) merge newKey
            replaceContents = false
          case _ =>
            // one of the two arrays isn't a proper classified array,
            // we must just replace the older with the newer
            replaceContents = true
        }
      }
      val docBKeyExists = (docB \ key).toOption.isDefined
      if (!docBKeyExists) {
        res = mergeKey(key, docA, res)
      } else for {
        sourceA <- (docA \ "source" \ key).toOption orElse (docA \ "source" \ "$remaining").toOption
        sourceB <- (docB \ "source" \ key).toOption orElse (docB \ "source" \ "$remaining").toOption
        deliveredA = (sourceA \ "deliveredAt").extract[DateTime]
        deliveredB = (sourceB \ "deliveredAt").extract[DateTime]
        roleA = (sourceA \ "role").extract[String]
        roleB = (sourceB \ "role").extract[String]
        aIsNewerThanB = deliveredA.isAfter(deliveredB)
        aIsAuthorisedToReplaceB = AuthorityRoles.indexOf(roleA) >= AuthorityRoles.indexOf(roleB)
        aBias = AuthorityRoles.indexOf(roleA) - AuthorityRoles.indexOf(roleB)
        if replaceContents && ((aIsNewerThanB && aIsAuthorisedToReplaceB) || aBias > 0)
      } res = mergeKey(key, docA, res)
    }
    res
  }

  private def mergeKey(key: String, from: JValue, to: JValue) = {
    val fromKey: JValue = key -> from \ key
    val fromSourceKey: JValue = "source" -> (key -> from \ "source" \ key)
    to merge fromKey merge fromSourceKey
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
