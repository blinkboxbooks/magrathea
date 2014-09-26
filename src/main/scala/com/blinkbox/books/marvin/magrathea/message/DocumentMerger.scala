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
    s"Cannot merge documents with different schemas:\n- ${compact(render(dA))}\n- ${compact(render(dB))}")

  case class DifferentClassificationException(dA: JValue, dB: JValue) extends RuntimeException(
    s"Cannot merge documents with different classifications:\n- ${compact(render(dA))}\n- ${compact(render(dB))}")
  
  case class MissingSourceException(d: JValue) extends RuntimeException(
    s"Cannot merge document without 'source' field: ${compact(render(d))}")

  case class IncorrectDocumentType(d: JValue) extends RuntimeException(
    s"The document has to be a JObject containing fields: ${compact(render(d))}")

  case class NotClassifiedException(d: JValue) extends RuntimeException(
    s"Cannot merge a non-classified array: ${compact(render(d))}")

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

    val dA = annotate(purify(docA))
    val dB = annotate(purify(docB))
    val src = (dA \ "source") merge (dB \ "source")
    schema merge classification merge doMerge(dA, dB, src)
  }

  /** Strip the keys that are irrelevant to the merge process. */
  private def purify(doc: JValue): JValue = doc.removeDirectField("_id").removeDirectField("_rev")
    .removeDirectField("$schema").removeDirectField("classification")

  /** Change the document so that every field has a reference to its source. */
  private def annotate(doc: JValue): JValue = {
    val oldSrc = (doc \ "source").toOption.getOrElse(throw MissingSourceException(doc))
    val srcHash = oldSrc.sha1
    val newSrc: JValue = srcHash -> oldSrc
    var annotated = false
    def doAnnotate(doc: JValue): JValue = doc match {
      case JObject(xs) => JObject(annotateFields(xs))
      case JArray(xs) => if (isClassified(xs)) JArray(annotateArrays(xs)) else annotateValue(xs)
      case x =>
        annotated = true
        annotateValue(x)
    }
    def annotateFields(vs: List[JField]): List[JField] = vs match {
      case Nil => Nil
      case (xn, xv) :: xs => JField(xn, if (isAnnotated(xv)) xv else doAnnotate(xv)) :: annotateFields(xs)
    }
    def annotateArrays(vs: List[JValue]): List[JValue] = vs match {
      case Nil => Nil
      case x :: xs => (if (isAnnotated(x)) x else annotateValue(x)) :: annotateArrays(xs)
    }
    def annotateValue(v: JValue): JValue = ("value" -> v) ~ ("source" -> srcHash)
    def isAnnotated(f: JValue): Boolean = (f \ "value" != JNothing) && (f \ "source" != JNothing)
    def isClassified(arr: List[JValue]): Boolean = (arr.size > 0) && arr.forall(_ \ "classification" != JNothing)
    doAnnotate(doc.removeDirectField("source")).replaceDirectField("source", if (annotated) newSrc else oldSrc)
  }

  private def doMerge(valA: JValue, valB: JValue, src: JValue): JValue = (valA, valB) match {
    case (JObject(xs), JObject(ys)) => JObject(mergeFields(xs, ys, src))
    case (JArray(xs), JArray(ys)) => JArray(mergeArrays(xs, ys, src))
    case (JNothing, y) => y
    case (x, JNothing) => x
    case (_, y) => y
  }

  private def mergeFields(vsA: List[JField], vsB: List[JField], src: JValue): List[JField] = {
    def mergeRec(xleft: List[JField], yleft: List[JField]): List[JField] = xleft match {
      case Nil => yleft.map { case (key, value) =>
        logger.debug("Adding '{}'", key)
        key -> getFieldWithSource(value, src)
      }
      case (xn, xv) :: xs => yleft find (_._1 == xn) match {
        case Some(y @ (yn, yv)) =>
          mergeStrategyForA(xv, yv, src) match {
            case MergeStrategy.Merge =>
              logger.debug("Merging '{}'", xn)
              JField(xn, doMerge(xv, yv, src)) :: mergeRec(xs, yleft filterNot (_ == y))
            case MergeStrategy.Replace =>
              logger.debug("Replacing '{}'", xn)
              JField(xn, getFieldWithSource(yv, src)) :: mergeRec(xs, yleft filterNot (_ == y))
            case MergeStrategy.Keep =>
              logger.debug("Keeping '{}' unchanged", xn)
              JField(xn, xv) :: mergeRec(xs, yleft filterNot (_ == y))
          }
        case None => JField(xn, xv) :: mergeRec(xs, yleft)
      }
    }
    mergeRec(vsA, vsB)
  }

  private def mergeArrays(vsA: List[JValue], vsB: List[JValue], src: JValue): List[JValue] = {
    def mergeRec(xleft: List[JValue], yleft: List[JValue]): List[JValue] = xleft match {
      case Nil => yleft.map { v =>
        logger.debug("Adding '{}'", v)
        getFieldWithSource(v, src)
      }
      case x :: xs => yleft find (_ \ "classification" == x \ "classification") match {
        case Some(y) =>
          logger.debug("")
          doMerge(x, y, src) :: mergeRec(xs, yleft filterNot (_ == y))
        case None => x :: mergeRec(xs, yleft)
      }
    }
    // we only merge classified arrays, otherwise we throw an exception
    (prepareClassifiedArray(vsA), prepareClassifiedArray(vsB)) match {
      case (Some(cA), Some(cB)) =>
        logger.debug("Merging classified arrays")
        mergeRec(cA, cB)
      case (None, Some(_)) => throw NotClassifiedException(vsA)
      case (Some(_), None) => throw NotClassifiedException(vsB)
      case _ => throw NotClassifiedException(vsA)
    }
  }

  /** Checks whether the given array can be a classified array and returns its unique classifications. */
  private def prepareClassifiedArray(arr: List[JValue]): Option[List[JValue]] = {
    // for an array to be classified, all of its items must have a classification field.
    if (arr.forall(_ \ "classification" != JNothing)) {
      // creating a unique classified array by merging any duplicates
      val seen = mutable.Map.empty[JValue, JValue]
      for (x <- arr) {
        val key = x \ "classification"
        seen += key -> seen.get(key).map(_ merge x).getOrElse(x)
      }
      Some(seen.values.toList)
    } else None
  }

  /** There can be three cases: merge, replace or keep. Merge has priority over replace. */
  private def mergeStrategyForA(vA: JValue, vB: JValue, src: JValue): MergeStrategy = {
    val canMergeA = vA.isInstanceOf[JObject] || vA.isInstanceOf[JArray]
    val canMergeB = vB.isInstanceOf[JObject] || vB.isInstanceOf[JArray]
    val canMerge = canMergeA && canMergeB
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

  /** Returns a json source field of the given key, ready to be merged with an element */
  private def getFieldWithSource(value: JValue, src: JValue): JObject = ("value" -> value) ~ ("source" -> src)
}
