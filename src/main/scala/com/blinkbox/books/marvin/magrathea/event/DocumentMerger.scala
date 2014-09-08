package com.blinkbox.books.marvin.magrathea.event

import com.blinkbox.books.json.DefaultFormats
import org.joda.time.DateTime
import org.json4s._
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object DocumentMerger {
  case class DifferentClassificationException(c1: JValue, c2: JValue) extends RuntimeException(
    s"Cannot merge documents with different classifications:\n- ${compact(render(c1))}\n- ${compact(render(c2))}")

  implicit val json4sJacksonFormats = DefaultFormats
  private val StaticKeys = Seq("source", "classification", "$schema")
  private val AuthorityRoles = Seq("publisher_ftp", "content_manager")
  // We don't compare these fields, as they're not source data or are important to the comparison
  private val noStaticKeys: ((String, Any)) => Boolean = { case (key, _) => !StaticKeys.contains(key) }

  def merge(docA: JValue, docB: JValue): JValue = {
    if ((docA \ "classification") != (docB \ "classification"))
      throw DifferentClassificationException(docA \ "classification", docB \ "classification")

    // "b" will be emitted, so update it with data from "a". If an element of "a"
    // is newer, ensure there's an object representing the times things were updated.
    var res = docB
    val a: Map[String, Any] = docA.asInstanceOf[JObject].values
    a.withFilter(noStaticKeys).foreach { case(key, value) =>
      val aIsBetterThanB = for {
        sourceA <- (docA \ "source" \ key).toOption orElse (docA \ "source" \ "$remaining").toOption
        sourceB <- (docB \ "source" \ key).toOption orElse (docB \ "source" \ "$remaining").toOption
        deliveredA = (sourceA \ "deliveredAt").extract[DateTime]
        deliveredB = (sourceB \ "deliveredAt").extract[DateTime]
        docBKeyExists = (docB \ key).toOption.isDefined
      } yield deliveredA.isAfter(deliveredB) || !docBKeyExists

      val aIsAuthorisedToReplaceB = for {
        sourceA <- (docA \ "source" \ key).toOption orElse (docA \ "source" \ "$remaining").toOption
        sourceB <- (docB \ "source" \ key).toOption orElse (docB \ "source" \ "$remaining").toOption
        roleA = (sourceA \ "role").extract[String]
        roleB = (sourceB \ "role").extract[String]
        docBKeyExists = (docB \ key).toOption.isDefined
      } yield AuthorityRoles.indexOf(roleA) >= AuthorityRoles.indexOf(roleB) || !docBKeyExists

      var replaceContents = true

      // deep merge
      for {
        keyA <- (docA \ key).toOption
        hasChildren = keyA.children.size > 0
        hasClassification = (keyA \ "classification").toOption.isDefined
        if hasChildren && hasClassification
      } {
        //
      }

      for {
        aIsBetterThanB <- aIsBetterThanB
        aIsAuthorisedToReplaceB <- aIsAuthorisedToReplaceB
        if aIsBetterThanB && aIsAuthorisedToReplaceB && replaceContents
      } {
        val newKey: JValue = key -> docA \ key
        val newSourceKey: JValue = "source" -> (key -> docA \ "source" \ key)
        res = res merge newKey merge newSourceKey
      }
    }
    res
  }

//  private def prepareClassifiedArray(array: JValue, parentSource: JValue): Option[JValue] = {
//    var prepared: JValue = ("source" -> "")
//  }
}
