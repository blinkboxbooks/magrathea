package com.blinkbox.books.marvin.magrathea.message

import org.json4s.JsonAST._

object Checker {
  sealed trait Reason
  case object NoTitle extends Reason
  case object Unavailable extends Reason
  case object Unsuppliable extends Reason
  case object Unsellable extends Reason
  case object NoPublisher extends Reason
  case object NoCover extends Reason
  case object NoEpub extends Reason
  case object NotEnglish extends Reason
  case object NoDescription extends Reason
  case object NoUsablePrice extends Reason
  case object Racy extends Reason

  type Checker = JValue => Option[Reason]

  def apply(f: Checker) = f

  def hasItemInSet[T](set: Set[T], item: T): Boolean = set.contains(item)

  def hasRestrictedImprint(set: Set[String], item: String): Boolean = hasItemInSet(set, item)

  def hasRestrictedPublisher(set: Set[String], item: String): Boolean = hasItemInSet(set, item)

  def subject2Tuple(subject: JValue): (String, String) =
    subject match {
      case JObject(fields) => (fields.find(_._1 == "type"), fields.find(_._1 == "code")) match {
        case (Some((_, JString(typeVal))), Some((_, JString(codeVal)))) => (typeVal, codeVal)
        case _ => throw new RuntimeException("The book's subjects json format does not match with type / code.")
      }
      case _ => throw new RuntimeException("The book's subjects json format does not match with type / code.")
    }

  def hasRestrictedSubject(set: Set[(String, String)], subjects: List[JValue]): Boolean =
    subjects.map(subject2Tuple).exists(hasItemInSet(set, _))

  def checkRights(world: JValue, gb: JValue, row: JValue, reason: Reason): Option[Reason] =
    (world, gb, row) match {
      case (JBool(includesWorld), _, _) if !includesWorld => Some(reason)
      case (_, JBool(includesGb), _) if !includesGb => Some(reason)
      case (_, JNothing, JBool(includesRestOfWorld)) if !includesRestOfWorld => Some(reason)
      case _ => None
    }

  def checkClassification(bestClassification: JValue, classifications: JValue, reason: Reason,
    predicate: (JValue, List[JField]) => Boolean): Option[Reason] = {
    (bestClassification, classifications) match {
      case (JArray(best :: Nil), JObject(fields)) =>
        val classificationFields = fields.collect {
          case c @ ("classification", JArray(classification)) if classification.contains(best) => c
        }
        if (predicate(best, classificationFields)) None else Some(reason)
      case (JArray(best :: Nil), JArray(classification)) =>
        val classificationFields = classification.map("classification" -> _)
        if (predicate(best, classificationFields) && classification.contains(best)) None else Some(reason)
      case _ => Some(reason)
    }
  }

  def classificationExists(fields: List[JField], predicate: JValue => Boolean): Boolean =
    fields.exists {
      case ("classification", JArray(classification)) => classification.exists(predicate)
      case _ => false
    }
}
