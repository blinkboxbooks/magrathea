package com.blinkbox.books.marvin.magrathea.message

import com.blinkbox.books.json.DefaultFormats
import com.typesafe.scalalogging.slf4j.Logger
import org.joda.time.DateTime
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory
import shapeless.Typeable._

import scala.collection.mutable
import scala.language.implicitConversions
import com.blinkbox.books.marvin.magrathea.Json4sExtensions._

/**
 * Merging algorithm rules:
 * http://jira.blinkbox.local/confluence/display/QUILL/Merging+rules
 */
object DocumentMerger {
  case class DifferentClassificationException(cA: JValue, cB: JValue) extends RuntimeException(
    s"Cannot merge documents with different classifications:\n- ${compact(render(cA))}\n- ${compact(render(cB))}")

  case class DocWithSrc[T](doc: T, src: JValue)

  implicit val json4sJacksonFormats = DefaultFormats
  private val StaticKeys = Seq("source", "classification", "$schema")
  private val AuthorityRoles = Seq("publisher_ftp", "content_manager")
  private val logger: Logger = Logger(LoggerFactory getLogger getClass.getName)

  def merge(docA: JValue, docB: JValue): JValue = {
    if ((docA \ "classification") != (docB \ "classification"))
      throw DifferentClassificationException(docA \ "classification", docB \ "classification")

    val srcA = (docA \ "source").cast[JObject].getOrElse(throw new IllegalArgumentException(
      s"Cannot find document source in ${compact(render(docA))}"))
    val srcB = (docB \ "source").cast[JObject].getOrElse(throw new IllegalArgumentException(
      s"Cannot find document source in ${compact(render(docB))}"))

    val res = doMerge(purify(docA), purify(docB), srcA, srcB, JNothing)
    res.doc merge res.src
  }

  private def purify(doc: JValue): JValue = doc.removeDirectField("_id").removeDirectField("_rev")

  private def doMerge(valA: JValue, valB: JValue, srcA: JObject, srcB: JObject, srcAcc: JValue,
                      key: Option[String] = None): DocWithSrc[JValue] = (valA, valB) match {
    case (JObject(xs), JObject(ys)) =>
      val mergeRes = mergeFields(xs, ys, srcA, srcB, srcAcc, key)
      DocWithSrc(JObject(mergeRes.doc), mergeRes.src)
    case (JArray(xs), JArray(ys)) =>
      val mergeRes = mergeArrays(xs, ys, srcA, srcB, srcAcc, key)
      DocWithSrc(JArray(mergeRes.doc), mergeRes.src)
    case (JNothing, y) => DocWithSrc(y, srcAcc)
    case (x, JNothing) => DocWithSrc(x, srcAcc)
    case (x, y) => DocWithSrc(y, srcAcc)
  }

  private def mergeFields(vsA: List[JField], vsB: List[JField], srcA: JObject, srcB: JObject, srcAcc: JValue,
                          key: Option[String]): DocWithSrc[List[JField]] = {
    def mergeRec(xleft: List[JField], yleft: List[JField], srcAcc: JValue): DocWithSrc[List[JField]] = xleft match {
      case Nil =>
        val src = yleft.foldLeft(srcAcc)((acc, cur) => acc merge getKeySourceField(cur._1, srcB))
        DocWithSrc(yleft, src)
      case (xn, xv) :: xs => yleft find (_._1 == xn) match {
        case Some(y @ (yn, yv)) =>
          if (StaticKeys.contains(xn)) {
            val mergeRecRes = mergeRec(xs, yleft filterNot (_ == y), srcAcc)
            DocWithSrc(JField(xn, xv) :: mergeRecRes.doc, mergeRecRes.src)
          } else mergeOrReplaceA(xn, xv, yv, srcA, srcB) match {
            case Some(true) =>
              logger.debug(s"Merging '$xn'")
              val mergeRes = doMerge(xv, yv, srcA, srcB, srcAcc, Some(xn))
              val mergeRecRes = mergeRec(xs, yleft filterNot (_ == y), srcAcc)
              DocWithSrc(JField(xn, mergeRes.doc) :: mergeRecRes.doc, mergeRes.src merge mergeRecRes.src)
            case Some(false) =>
              val src = getKeySourceField(xn, srcB)
              logger.debug(s"Replacing '$xn' (src: '${compact(render(src))})")
              val mergeRecRes = mergeRec(xs, yleft filterNot (_ == y), srcAcc)
              DocWithSrc(JField(xn, yv) :: mergeRecRes.doc, mergeRecRes.src merge src)
            case None =>
              logger.debug(s"Keeping '$xn' unchanged")
              val mergeRecRes = mergeRec(xs, yleft filterNot (_ == y), srcAcc)
              DocWithSrc(JField(xn, xv) :: mergeRecRes.doc, mergeRecRes.src)
          }
        case None =>
          val mergeRecRes = mergeRec(xs, yleft, srcAcc)
          DocWithSrc(JField(xn, xv) :: mergeRecRes.doc, mergeRecRes.src)
      }
    }
    mergeRec(vsA, vsB, srcAcc)
  }

  private def mergeArrays(vsA: List[JValue], vsB: List[JValue], srcA: JObject, srcB: JObject, srcAcc: JValue,
                          key: Option[String]): DocWithSrc[List[JValue]] = {
    def mergeRec(xleft: List[JValue], yleft: List[JValue], srcAcc: JValue): DocWithSrc[List[JValue]] = xleft match {
      case Nil =>
        val src = key.map(k => getSourceField(k, srcB)).getOrElse(JNothing)
        DocWithSrc(yleft.map(_ merge src), srcAcc)
      case x :: xs => yleft find (_ \ "classification" == x \ "classification") match {
        case Some(y) =>
          val mergeRes = doMerge(x, y, srcA, srcB, srcAcc)
          val mergeRecRes = mergeRec(xs, yleft filterNot (_ == y), srcAcc)
          DocWithSrc((mergeRes.doc merge mergeRes.src) :: mergeRecRes.doc, mergeRecRes.src)
        case None =>
          val mergeRecRes = mergeRec(xs, yleft, srcAcc)
          DocWithSrc(x :: mergeRecRes.doc, mergeRecRes.src)
      }
    }
    def replaceArray(xA: List[JValue], xB: List[JValue]): DocWithSrc[List[JValue]] = key match {
      case Some(k) =>
        logger.debug(s"Replacing array '$k'")
        if (canReplaceA(k, xA, xB, srcA, srcB)) DocWithSrc(xB, srcAcc merge getKeySourceField(k, srcB))
        else DocWithSrc(xA, srcAcc)
      case None =>
        logger.debug("Undefined behaviour: keeping the current array")
        DocWithSrc(xA, srcAcc)
    }
    // we only merge classified arrays, otherwise we replace and keep the newest
    (prepareClassifiedArray(vsA), prepareClassifiedArray(vsB)) match {
      case (Some(cA), Some(cB)) =>
        logger.debug(s"Merging classified array '${key.getOrElse("N/A")}'")
        mergeRec(cA, cB, srcAcc)
      case _ => replaceArray(vsA, vsB)
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

  /** There can be three cases: merge, replace or nothing. Merge has priority over replace. Returns true for merge.  */
  private def mergeOrReplaceA(key: String, vA: JValue, vB: JValue, srcA: JObject, srcB: JObject): Option[Boolean] = {
    val canMergeA = vA.isInstanceOf[JObject] || vA.isInstanceOf[JArray]
    val canMergeB = vB.isInstanceOf[JObject] || vB.isInstanceOf[JArray]
    val canMerge = canMergeA && canMergeB
    if (canMerge) Some(true)
    else if (canReplaceA(key, vA, vB, srcA, srcB)) Some(false)
    else None
  }

  /** Returns whether key in document A can be replaced with key in document B. */
  private def canReplaceA(key: String, vA: JValue, vB: JValue, srcA: JObject, srcB: JObject): Boolean = {
    val keySourceA = getKeySource(key, srcA)
    val keySourceB = getKeySource(key, srcB)
    val deliveredA = (keySourceA \ "deliveredAt").extract[DateTime]
    val deliveredB = (keySourceB \ "deliveredAt").extract[DateTime]
    val roleA = (keySourceA \ "role").extract[String]
    val roleB = (keySourceB \ "role").extract[String]
    val bIsNewerThanA = deliveredB.isAfter(deliveredA)
    val bIsAuthorisedToReplaceA = AuthorityRoles.indexOf(roleB) >= AuthorityRoles.indexOf(roleA)
    val bRoleBias = AuthorityRoles.indexOf(roleB) - AuthorityRoles.indexOf(roleA)
    (bIsNewerThanA && bIsAuthorisedToReplaceA) || bRoleBias > 0
  }

  /** Returns the source of a given field or the document source if the field does not have a different one. */
  private def getKeySource(key: String, src: JValue): JValue =
    ((src \ key).toOption orElse (src \ "$remaining").toOption).getOrElse(
      throw new IllegalArgumentException(s"Cannot find source for '$key': ${compact(render(src))}"))

  /** Returns a json source field of the given key, ready to be merged with the rest of the source. */
  private def getKeySourceField(key: String, src: JValue): JValue = "source" -> (key -> getKeySource(key, src))

  /** Returns a json source field of the given key, ready to be merged with an element */
  private def getSourceField(key: String, src: JValue): JValue = "source" -> ("$remaining" -> getKeySource(key, src))
}
