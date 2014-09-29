package com.blinkbox.books.marvin.magrathea.message

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.Json4sExtensions._
import com.blinkbox.books.marvin.magrathea.message.DocumentMerger.MergeStrategy.MergeStrategy
import com.typesafe.scalalogging.slf4j.Logger
import org.joda.time.DateTime
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.language.implicitConversions

/**
 * Merging algorithm rules:
 * http://jira.blinkbox.local/confluence/display/QUILL/Merging+rules
 */
object DocumentMerger {
  object MergeStrategy extends Enumeration {
    type MergeStrategy = Value
    val Merge, Replace, Keep = Value
  }

  case class Source(src: JValue) {
    /** There can be three cases: merge, replace or keep. Merge has priority over replace. */
    def mergeStrategyForX(x: JValue, y: JValue): MergeStrategy = {
      val canMerge = !DocumentAnnotator.isAnnotated(x) && !DocumentAnnotator.isAnnotated(y)
      if (canMerge) MergeStrategy.Merge
      else if (canReplaceX(x, y)) MergeStrategy.Replace
      else MergeStrategy.Keep
    }

    /** Returns whether key in document X can be replaced with key in document Y. */
    def canReplaceX(x: JValue, y: JValue): Boolean = {
      val srcX = src \ (x \ "source").extract[String]
      val srcY = src \ (y \ "source").extract[String]
      val deliveredX = (srcX \ "deliveredAt").extract[DateTime]
      val deliveredY = (srcY \ "deliveredAt").extract[DateTime]
      val roleX = (srcX \ "role").extract[String]
      val roleY = (srcY \ "role").extract[String]
      val yIsNewerThanX = deliveredY.isAfter(deliveredX)
      val yIsAuthorisedToReplaceX = AuthorityRoles.indexOf(roleY) >= AuthorityRoles.indexOf(roleX)
      val yRoleBias = AuthorityRoles.indexOf(roleY) - AuthorityRoles.indexOf(roleX)
      (yIsNewerThanX && yIsAuthorisedToReplaceX) || yRoleBias > 0
    }

    /** Returns the merged document along with the unique sources used. */
    def withDoc(doc: JValue): JValue = {
      val srcField: JValue = "source" -> (src match {
        case JObject(l) => JObject(l.filter { case (key, _) => doc \\ "source" match {
          case JObject(sources) => sources.exists(_._2 == JString(key))
          case x => x == JString(key)
        }})
        case _ => src
      })
      doc merge srcField
    }
  }

  case class DifferentSchemaException(dA: JValue, dB: JValue) extends RuntimeException(
    s"Cannot merge documents with different schemas:\n- ${compact(dA)}\n- ${compact(dB)}")

  case class DifferentClassificationException(dA: JValue, dB: JValue) extends RuntimeException(
    s"Cannot merge documents with different classifications:\n- ${compact(dA)}\n- ${compact(dB)}")
  
  implicit val json4sJacksonFormats = DefaultFormats
  private val logger: Logger = Logger(LoggerFactory getLogger getClass.getName)
  private val AuthorityRoles = Seq("publisher_ftp", "content_manager")

  def merge(docA: JValue, docB: JValue): JValue = {
    val schema: JValue = "$schema" -> (
      if ((docA \ "$schema") == (docB \ "$schema")) docA \ "$schema"
      else throw DifferentSchemaException(docA \ "$schema", docB \ "$schema"))

    val classification: JValue = "classification" -> (
      if ((docA \ "classification") == (docB \ "classification")) docA \ "classification"
      else throw DifferentClassificationException(docA \ "classification", docB \ "classification"))

    val annotatedA = DocumentAnnotator.annotate(purify(docA))
    val annotatedB = DocumentAnnotator.annotate(purify(docB))
    val src = Source((annotatedA \ "source") merge (annotatedB \ "source"))
    logger.debug("Starting document merging...")
    val result = doMerge(annotatedA.removeDirectField("source"), annotatedB.removeDirectField("source"), src)
    logger.debug("Finished document merging")
    schema merge classification merge src.withDoc(result)
  }

  /** Strip the keys that are irrelevant to the merge process. */
  private def purify(doc: JValue): JValue = doc.removeDirectField("_id").removeDirectField("_rev")
    .removeDirectField("$schema").removeDirectField("classification")

  private def doMerge(valA: JValue, valB: JValue, src: Source): JValue = (valA, valB) match {
    case (JObject(xs), JObject(ys)) => JObject(mergeFields(xs, ys, src))
    case (JArray(xs), JArray(ys)) => JArray(mergeClassifiedArrays(xs, ys, src))
    case (JNothing, y) => y
    case (x, JNothing) => x
    case (_, y) => y
  }

  private def mergeFields(vsA: List[JField], vsB: List[JField], src: Source): List[JField] = {
    def mergeRec(xleft: List[JField], yleft: List[JField]): List[JField] = xleft match {
      case Nil => yleft
      case (xn, xv) :: xs => yleft find (_._1 == xn) match {
        case Some(y @ (yn, yv)) => src.mergeStrategyForX(xv, yv) match {
          case MergeStrategy.Merge =>
            logger.debug("Merging field '{}'", xn)
            JField(xn, doMerge(xv, yv, src)) :: mergeRec(xs, yleft filterNot (_ == y))
          case MergeStrategy.Replace =>
            logger.debug("Replacing field '{}'", xn)
            JField(xn, yv) :: mergeRec(xs, yleft filterNot (_ == y))
          case MergeStrategy.Keep =>
            logger.debug("Keeping field '{}'", xn)
            JField(xn, xv) :: mergeRec(xs, yleft filterNot (_ == y))
        }
        case None => JField(xn, xv) :: mergeRec(xs, yleft)
      }
    }
    logger.debug("Merging fields...")
    mergeRec(vsA, vsB)
  }

  private def mergeClassifiedArrays(vsA: List[JValue], vsB: List[JValue], src: Source): List[JValue] = {
    def mergeRec(xleft: List[JValue], yleft: List[JValue]): List[JValue] = xleft match {
      case Nil => yleft
      case x :: xs => yleft find (_ \ "value" \ "classification" == x \ "value" \ "classification") match {
        case Some(y) =>
          if (src.canReplaceX(x, y)) {
            logger.debug("Replacing classification '{}'", compact(x \ "value" \ "classification"))
            y :: mergeRec(xs, yleft filterNot (_ == y))
          } else {
            logger.debug("Keeping classification '{}'", compact(x \ "value" \ "classification"))
            x :: mergeRec(xs, yleft filterNot (_ == y))
          }
        case None => x :: mergeRec(xs, yleft)
      }
    }
    logger.debug("Merging classified arrays...")
    mergeRec(vsA, vsB)
  }
}
