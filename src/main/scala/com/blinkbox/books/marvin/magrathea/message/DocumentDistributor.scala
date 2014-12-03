package com.blinkbox.books.marvin.magrathea.message

import java.util.concurrent.Executors

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.message.DocumentDistributor.Reason
import com.blinkbox.books.marvin.magrathea.message.DocumentDistributor.Reason.Reason
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
  case class Status(sellable: Boolean, reasons: Option[List[Reason]])
}

class DocumentDistributor(config: DistributorConfig, schemas: SchemaConfig)
  extends Json4sJacksonSupport with JsonMethods with StrictLogging {
  import com.blinkbox.books.marvin.magrathea.message.DocumentStatus._

  implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))
  implicit val json4sJacksonFormats = DefaultFormats
  private val checkers: List[Checker] = TitleChecker :: AvailabilityChecker :: SuppliableChecker ::
    SellableChecker :: PublisherChecker :: CoverChecker :: EpubChecker :: EnglishChecker ::
    DescriptionChecker :: UsablePriceChecker :: RacyTitleChecker :: Nil

  /** TODO implement this */
  def sendDistributionInformation(document: JValue): Future[Unit] = Future {
    document \ "$schema" match {
      case JString(schema) if schema == schemas.book => ()
      case JString(schema) if schema == schemas.contributor => ()
      case x => throw new IllegalArgumentException(s"Cannot get distribution information from unsupported schema: $x")
    }
  }

  def status(doc: JValue): DocumentDistributor.Status = {
    val reasons = checkers.foldLeft(List.empty[Reason]) { (acc, check) =>
      check(doc).fold(acc)(_ ::: acc)
    }
    DocumentDistributor.Status(sellable = reasons.isEmpty, if (reasons.nonEmpty) Some(reasons) else None)
  }
}

object DocumentStatus extends v2.JsonSupport {
  import org.json4s.JsonDSL._

  type Checker = JValue => Option[List[Reason.Value]]

  private val rightsChecker: (JValue, JValue, JValue, Reason.Value) => Option[List[Reason.Value]] = {
    case (JBool(world), _, _, reason) if !world => Some(List(reason))
    case (_, JBool(gb), _, reason) if !gb => Some(List(reason))
    case (_, JNothing, JBool(row), reason) if !row => Some(List(reason))
    case _ => None
  }

  private val classificationChecker: (JValue, JValue, Reason.Value, => (JValue, List[JField]) => Boolean) => Option[List[Reason.Value]] = {
    case (JArray(best :: Nil), JObject(fields), reason, check) =>
      val fieldsWithBestClassification = fields.filter {
        case ("classification", JArray(classification)) => classification.contains(best)
        case _ => false
      }
      if (check(best, fieldsWithBestClassification)) None else Some(List(reason))
    case (JArray(best :: Nil), JArray(classification), reason, check) =>
      if (classification.contains(best) && check(best, classification.map("classification" -> _))) None else Some(List(reason))
    case (_, _, reason, _) => Some(List(reason))
  }

  private val classificationExists: (List[JField], JValue => Boolean) => Boolean = (fields, exists) =>
    fields.exists {
      case ("classification", JArray(classification)) => classification.exists(exists)
      case _ => false
    }

  val TitleChecker: Checker = _ \ "title" match {
    case JString(title) if title.nonEmpty => None
    case _ => Some(List(Reason.NoTitle))
  }

  val AvailabilityChecker: Checker = _ \\ "available" match {
    case JBool(available) if !available => Some(List(Reason.Unavailable))
    case JObject(fields) =>
      val allAvailable = fields.forall {
        case (_, JBool(available)) => available
        case _ => false
      }
      if (!allAvailable) Some(List(Reason.Unavailable)) else None
    case _ => None
  }

  val SuppliableChecker: Checker = doc => rightsChecker(doc \ "supplyRights" \ "WORLD",
    doc \ "supplyRights" \ "GB", doc \ "supplyRights" \ "ROW", Reason.Unsuppliable)

  val SellableChecker: Checker = doc => rightsChecker(doc \ "salesRights" \ "WORLD",
    doc \ "salesRights" \ "GB", doc \ "salesRights" \ "ROW", Reason.Unsellable)

  val PublisherChecker: Checker = doc => (doc \ "publisher", doc \ "imprint") match {
    case (JString(publisher), _) if publisher.nonEmpty => None
    case (_, JString(imprint)) if imprint.nonEmpty => None
    case _ => Some(List(Reason.NoPublisher))
  }

  val CoverChecker: Checker = doc => {
    val frontCover: JValue = ("realm" -> "type") ~ ("id" -> "front_cover")
    doc \ "images" \\ "classification" match {
      case JArray(classification) if classification.contains(frontCover) => None
      case JObject(fields) =>
        val hasCover = fields.exists {
          case ("classification", JArray(classification)) => classification.contains(frontCover)
          case _ => false
        }
        if (!hasCover) Some(List(Reason.NoCover)) else None
      case _ => Some(List(Reason.NoCover))
    }
  }

  val EpubChecker: Checker = doc => classificationChecker(doc \ "media" \ "epubs" \ "best",
    doc \ "media" \ "epubs" \ "items" \\ "classification", Reason.NoEpub, (best, fieldsWithBestClassification) => {
      val containsSample = classificationExists(fieldsWithBestClassification, _ \ "id" == JString("sample"))
      val containsDrm = classificationExists(fieldsWithBestClassification, _ \ "id" == JString("full_bbbdrm"))
      containsSample && containsDrm
    })

  val EnglishChecker: Checker = _ \ "languages" match {
    case JArray(languages) if languages.contains(JString("eng")) => None
    case _ => Some(List(Reason.NotEnglish))
  }

  val DescriptionChecker: Checker = doc => classificationChecker(doc \ "descriptions" \ "best",
    doc \ "descriptions" \ "items" \\ "classification", Reason.NoDescription, (_, fieldsWithBestClassification) => {
      fieldsWithBestClassification.nonEmpty
    })

  val UsablePriceChecker: Checker = _ \ "prices" \ "includesTax" match {
    case JBool(includesTax) if !includesTax => None
    case JObject(fields) =>
      val usablePriceExists = fields.exists {
        case ("includesTax", JBool(includesTax)) => !includesTax
        case _ => false
      }
      if (!usablePriceExists) Some(List(Reason.NoUsablePrice)) else None
    case _ => Some(List(Reason.NoUsablePrice))
  }

  val RacyTitleChecker: Checker = doc => None
}
