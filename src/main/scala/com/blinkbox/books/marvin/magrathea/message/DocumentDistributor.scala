package com.blinkbox.books.marvin.magrathea.message

import java.util.concurrent.Executors

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.message.DocumentDistributor.Reason
import com.blinkbox.books.marvin.magrathea.message.DocumentDistributor.Reason.Reason
import com.blinkbox.books.marvin.magrathea.message.DocumentStatus.Checker.Checker
import com.blinkbox.books.marvin.magrathea.{DistributorConfig, SchemaConfig}
import com.blinkbox.books.spray.v2
import com.typesafe.scalalogging.StrictLogging
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.{ExecutionContext, Future}

object DocumentDistributor {
  object Reason extends Enumeration {
    type Reason = Value
    val NoTitle, Unavailable, Unsuppliable, Unsellable, NoPublisher, NoCover,
      NoEpub, NotEnglish, NoDescription, NoUsablePrice, Racy = Value
  }
  case class Status(sellable: Boolean, reasons: Option[Set[Reason]])
}

class DocumentDistributor(config: DistributorConfig, schemas: SchemaConfig)
  extends Json4sJacksonSupport with JsonMethods with StrictLogging {
  import com.blinkbox.books.marvin.magrathea.message.DocumentStatus._

  implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))
  implicit val json4sJacksonFormats = DefaultFormats
  private val checkers: Set[Checker] = Set(TitleChecker, AvailabilityChecker, SuppliableChecker,
    SellableChecker, PublisherChecker, CoverChecker, EpubChecker, EnglishChecker,
    DescriptionChecker, UsablePriceChecker, RacyTitleChecker)

  /** TODO implement this */
  def sendDistributionInformation(document: JValue): Future[Unit] = Future {
    document \ "$schema" match {
      case JString(schema) if schema == schemas.book => ()
      case JString(schema) if schema == schemas.contributor => ()
      case x => throw new IllegalArgumentException(s"Cannot get distribution information from unsupported schema: $x")
    }
  }

  def status(doc: JValue): DocumentDistributor.Status = {
    val reasons = checkers.foldLeft(Set.empty[Reason]) { (acc, check) =>
      check(doc).fold(acc)(acc + _)
    }
    DocumentDistributor.Status(sellable = reasons.isEmpty, if (reasons.nonEmpty) Some(reasons) else None)
  }
}

object DocumentStatus extends v2.JsonSupport {
  import org.json4s.JsonDSL._

  object Checker {
    type Checker = JValue => Option[Reason.Value]

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

    def checkRights(world: JValue, gb: JValue, row: JValue, reason: Reason.Value): Option[Reason.Value] =
      (world, gb, row) match {
        case (JBool(includesWorld), _, _) if !includesWorld => Some(reason)
        case (_, JBool(includesGb), _) if !includesGb => Some(reason)
        case (_, JNothing, JBool(includesRestOfWorld)) if !includesRestOfWorld => Some(reason)
        case _ => None
      }

    def checkClassification(bestClassification: JValue, classifications: JValue, reason: Reason.Value,
      predicate: (JValue, List[JField]) => Boolean): Option[Reason.Value] = {
      (bestClassification, classifications) match {
        case (JArray(best :: Nil), JObject(fields)) =>
          val classificationFields = fields.filter {
            case ("classification", JArray(classification)) => classification.contains(best)
            case _ => false
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

  import com.blinkbox.books.marvin.magrathea.message.DocumentStatus.Checker._

  private val RestrictedImprints = Set(
    "Xcite Books",
    "Total-E-Bound Publishing",
    "Cleis Press",
    "House of Erotica",
    "W&H Publishing",
    "Cambridge House",
    "Chimera Books",
    "AUK Adult",
    "Bruno Gmunder Digital"
  )

  private val RestrictedPublishers = Set(
    "Xcite Books",
    "Chimera eBooks Ltd"
  )

  private val RestrictedSubjects = Set(
    ("BISAC", "FIC027010"),
    ("BISAC", "FIC005000"),
    ("BISAC", "PHO023030"),
    ("BISAC", "PHO023050"),
    ("BIC", "FP")
  )

  val TitleChecker = Checker { doc =>
    doc \ "title" match {
      case JString(title) if title.nonEmpty => None
      case _ => Some(Reason.NoTitle)
    }
  }

  val AvailabilityChecker = Checker { doc =>
    doc \\ "available" match {
      case JBool(available) if !available => Some(Reason.Unavailable)
      case JObject(fields) =>
        val allAvailable = fields.forall {
          case (_, JBool(available)) => available
          case _ => false
        }
        if (!allAvailable) Some(Reason.Unavailable) else None
      case _ => None
    }
  }

  val SuppliableChecker = Checker { doc =>
    checkRights(
      doc \ "supplyRights" \ "WORLD",
      doc \ "supplyRights" \ "GB",
      doc \ "supplyRights" \ "ROW",
      Reason.Unsuppliable
    )
  }

  val SellableChecker = Checker { doc =>
    checkRights(
      doc \ "salesRights" \ "WORLD",
      doc \ "salesRights" \ "GB",
      doc \ "salesRights" \ "ROW",
      Reason.Unsellable
    )
  }

  val PublisherChecker = Checker { doc =>
    (doc \ "publisher", doc \ "imprint") match {
      case (JString(publisher), _) if publisher.nonEmpty => None
      case (_, JString(imprint)) if imprint.nonEmpty => None
      case _ => Some(Reason.NoPublisher)
    }
  }

  val CoverChecker = Checker { doc =>
    val frontCover: JValue = ("realm" -> "type") ~ ("id" -> "front_cover")
    doc \ "images" \\ "classification" match {
      case JArray(classification) if classification.contains(frontCover) => None
      case JObject(fields) =>
        val hasCover = fields.exists {
          case ("classification", JArray(classification)) => classification.contains(frontCover)
          case _ => false
        }
        if (!hasCover) Some(Reason.NoCover) else None
      case _ => Some(Reason.NoCover)
    }
  }

  val EpubChecker = Checker { doc =>
    checkClassification(
      doc \ "media" \ "epubs" \ "best",
      doc \ "media" \ "epubs" \ "items" \\ "classification",
      Reason.NoEpub,
      (best, classificationFields) => {
        val containsSample = classificationExists(classificationFields, _ \ "id" == JString("sample"))
        val containsDrm = classificationExists(classificationFields, _ \ "id" == JString("full_bbbdrm"))
        containsSample && containsDrm
      }
    )
  }

  val EnglishChecker = Checker { doc =>
    doc \ "languages" match {
      case JArray(languages) if languages.contains(JString("eng")) => None
      case _ => Some(Reason.NotEnglish)
    }
  }

  val DescriptionChecker = Checker { doc =>
    checkClassification(
      doc \ "descriptions" \ "best",
      doc \ "descriptions" \ "items" \\ "classification",
      Reason.NoDescription,
      (_, classificationFields) => {
        classificationFields.nonEmpty
      }
    )
  }

  val UsablePriceChecker = Checker { doc =>
    doc \ "prices" \ "includesTax" match {
      case JBool(includesTax) if !includesTax => None
      case JObject(fields) =>
        val usablePriceExists = fields.exists {
          case ("includesTax", JBool(includesTax)) => !includesTax
          case _ => false
        }
        if (!usablePriceExists) Some(Reason.NoUsablePrice) else None
      case _ => Some(Reason.NoUsablePrice)
    }
  }

  val RacyTitleChecker = Checker { doc =>
    (doc \ "imprint", doc \ "publisher", doc \ "subjects") match {
      case (JString(imprint), JString(publisher), JArray(subjects)) if
        hasRestrictedImprint(RestrictedImprints, imprint) ||
        hasRestrictedPublisher(RestrictedPublishers, publisher) ||
        hasRestrictedSubject(RestrictedSubjects, subjects) => Some(Reason.Racy)
      case _ => None
    }
  }
}
