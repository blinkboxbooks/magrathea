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

import scala.collection.mutable
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

  case class DifferentSchemaException(dA: JValue, dB: JValue) extends RuntimeException(
    s"Cannot merge documents with different schemas:\n- ${compact(dA)}\n- ${compact(dB)}")

  case class DifferentClassificationException(dA: JValue, dB: JValue) extends RuntimeException(
    s"Cannot merge documents with different classifications:\n- ${compact(dA)}\n- ${compact(dB)}")
  
  case class MissingSourceException(d: JValue) extends RuntimeException(
    s"Cannot merge document without 'source' field: ${compact(d)}")

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

    val annotatedA = annotate(purify(docA))
    val annotatedB = annotate(purify(docB))
    val source = (annotatedA \ "source") merge (annotatedB \ "source")
    logger.debug("Starting document merging...")
    val result = doMerge(annotatedA.removeDirectField("source"), annotatedB.removeDirectField("source"), source)
    logger.debug("Finished document merging")
    schema merge classification merge docWithUniqueSource(result, source)
  }

  /** Strip the keys that are irrelevant to the merge process. */
  private def purify(doc: JValue): JValue = doc.removeDirectField("_id").removeDirectField("_rev")
    .removeDirectField("$schema").removeDirectField("classification")

  /** Returns the merged document along with the unique sources used. */
  private def docWithUniqueSource(doc: JValue, source: JValue): JValue = {
    val srcField: JValue = "source" -> (source match {
      case JObject(l) => JObject(l.filter { case (key, _) => doc \\ "source" match {
        case JObject(sources) => sources.exists(_._2 == JString(key))
        case x => x == JString(key)
      }})
      case _ => source
    })
    doc merge srcField
  }

  /** For a value to be annotated, it has to have only two children: value and source. */
  private def isAnnotated(v: JValue): Boolean =
    (v.children.size == 2) && (v \ "value" != JNothing) && (v \ "source" != JNothing)

  /** For an array to be classified, all of its items must have a classification field. */
  private def isClassified(arr: List[JValue]): Boolean = (arr.size > 0) &&
    arr.forall(v => (v \ "classification" != JNothing) || (v \ "value" \ "classification" != JNothing))

  /** Change the document so that every field has a reference to its source. */
  private def annotate(doc: JValue): JValue = {
    def doAnnotate(doc: JValue, srcHash: String): JValue = doc match {
      case JObject(xs) => JObject(annotateFields(xs, srcHash))
      case JArray(xs) if isClassified(xs) => JArray(uniquelyClassify(annotateArrays(xs, srcHash)))
      case JArray(xs) => annotateValue(xs, srcHash)
      case x => if (isAnnotated(x)) x else annotateValue(x, srcHash)
    }
    def annotateFields(vs: List[JField], srcHash: String): List[JField] = vs match {
      case Nil => Nil
      case (xn, xv) :: xs if isAnnotated(xv) => JField(xn, xv) :: annotateFields(xs, srcHash)
      case (xn, xv) :: xs => JField(xn, doAnnotate(xv, srcHash)) :: annotateFields(xs, srcHash)
    }
    def annotateArrays(vs: List[JValue], srcHash: String): List[JValue] = vs match {
      case Nil => Nil
      case x :: xs if isAnnotated(x) => x :: annotateArrays(xs, srcHash)
      case x :: xs => annotateValue(x, srcHash) :: annotateArrays(xs, srcHash)
    }
    def annotateValue(v: JValue, srcHash: String): JValue = ("value" -> v) ~ ("source" -> srcHash)
    /** creating a unique classified array by merging any duplicates. */
    def uniquelyClassify(arr: List[JValue]): List[JValue] = {
      val seen = mutable.Map.empty[JValue, JValue]
      for (x <- arr) {
        val key = x \ "value" \ "classification"
        seen += key -> seen.get(key).map(_ merge x).getOrElse(x)
      }
      seen.values.toList
    }
    val oldSrc = (doc \ "source").toOption.getOrElse(throw MissingSourceException(doc))
    val result = doAnnotate(doc.removeDirectField("source"), oldSrc.sha1)
    val annotated = result \\ "source" match {
      case JObject(sources) => sources.exists(_._2 == JString(oldSrc.sha1))
      case x => x == JString(oldSrc.sha1)
    }
    if (doc.children.size == 1 || annotated) result.replaceDirectField("source", oldSrc.sha1 -> oldSrc)
    else result.replaceDirectField("source", oldSrc)
  }

  private def doMerge(valA: JValue, valB: JValue, src: JValue): JValue = (valA, valB) match {
    case (JObject(xs), JObject(ys)) => JObject(mergeFields(xs, ys, src))
    case (JArray(xs), JArray(ys)) => JArray(mergeClassifiedArrays(xs, ys, src))
    case (JNothing, y) => y
    case (x, JNothing) => x
    case (_, y) => y
  }

  private def mergeFields(vsA: List[JField], vsB: List[JField], src: JValue): List[JField] = {
    def mergeRec(xleft: List[JField], yleft: List[JField]): List[JField] = xleft match {
      case Nil => yleft
      case (xn, xv) :: xs => yleft find (_._1 == xn) match {
        case Some(y @ (yn, yv)) => mergeStrategyForA(xv, yv, src) match {
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

  private def mergeClassifiedArrays(vsA: List[JValue], vsB: List[JValue], src: JValue): List[JValue] = {
    def mergeRec(xleft: List[JValue], yleft: List[JValue]): List[JValue] = xleft match {
      case Nil => yleft
      case x :: xs => yleft find (_ \ "value" \ "classification" == x \ "value" \ "classification") match {
        case Some(y) =>
          if (canReplaceA(x, y, src)) {
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

  /** There can be three cases: merge, replace or keep. Merge has priority over replace. */
  private def mergeStrategyForA(vA: JValue, vB: JValue, src: JValue): MergeStrategy = {
    val canMerge = !isAnnotated(vA) && !isAnnotated(vB)
    if (canMerge) MergeStrategy.Merge
    else if (canReplaceA(vA, vB, src)) MergeStrategy.Replace
    else MergeStrategy.Keep
  }

  /** Returns whether key in document A can be replaced with key in document B. */
  private def canReplaceA(vA: JValue, vB: JValue, src: JValue): Boolean = {
    val srcA = src \ (vA \ "source").extract[String]
    val srcB = src \ (vB \ "source").extract[String]
    val deliveredA = (srcA \ "deliveredAt").extract[DateTime]
    val deliveredB = (srcB \ "deliveredAt").extract[DateTime]
    val roleA = (srcA \ "role").extract[String]
    val roleB = (srcB \ "role").extract[String]
    val bIsNewerThanA = deliveredB.isAfter(deliveredA)
    val bIsAuthorisedToReplaceA = AuthorityRoles.indexOf(roleB) >= AuthorityRoles.indexOf(roleA)
    val bRoleBias = AuthorityRoles.indexOf(roleB) - AuthorityRoles.indexOf(roleA)
    (bIsNewerThanA && bIsAuthorisedToReplaceA) || bRoleBias > 0
  }
}
