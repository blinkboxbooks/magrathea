package com.blinkbox.books.marvin.magrathea.message

import java.nio.charset.Charset
import java.util.concurrent.ForkJoinPool

import akka.actor.ActorRef
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.SchemaConfig
import com.blinkbox.books.marvin.magrathea.message.Checker._
import com.blinkbox.books.marvin.magrathea.message.DocumentDistributor.Status
import com.blinkbox.books.messaging._
import com.blinkbox.books.spray.v2
import com.typesafe.scalalogging.StrictLogging
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.{ExecutionContext, Future}

object DocumentDistributor {
  case class Status(usable: Boolean, reasons: Set[Reason]) {
    val toJson: JValue = "distributionStatus" -> (
      ("usable" -> usable) ~ ("reasons" -> reasons.map { reason =>
        reason.getClass.getName.split("\\$").last
      })
    )
  }
}

class DocumentDistributor(publisher: ActorRef, schemas: SchemaConfig)
  extends Json4sJacksonSupport with JsonMethods with StrictLogging {
  import com.blinkbox.books.marvin.magrathea.message.DocumentStatus._

  private implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(new ForkJoinPool()))
  override implicit val json4sJacksonFormats = DefaultFormats
  private val checkers: Set[Checker] = Set(TitleChecker, AvailabilityChecker, SuppliableChecker,
    SellableChecker, PublisherChecker, CoverChecker, EpubChecker, EnglishChecker,
    DescriptionChecker, UsablePriceChecker, RacyTitleChecker)

  def sendDistributionInformation(document: JValue): Future[Unit] = Future {
    document \ "$schema" match {
      case JString(schema) if schema == schemas.book || schema == schemas.contributor =>
        val json = compact(render(document))
        val contentType = ContentType(mediaTypeFor(schema), Some(Charset.forName("UTF-8")))
        publisher ! Event(EventHeader("magrathea"), EventBody(json.getBytes, contentType))
      case JString(x) => throw new IllegalArgumentException(s"Cannot send distribution information from unsupported schema: $x")
      case _ => throw new IllegalArgumentException("Cannot send distribution information: document schema is missing.")
    }
  }

  private def mediaTypeFor(schema: String): MediaType = MediaType(
    if (schema == schemas.book) s"application/vnd.blinkbox.books.$schema"
    else if (schema == schemas.contributor) s"application/vnd.blinkbox.contributor.$schema"
    else ""
  )

  def status(document: JValue): Status =
    document \ "$schema" match {
      case JString(schema) if schema == schemas.book =>
        val reasons = checkers.foldLeft(Set.empty[Reason]) { (acc, check) =>
          check(document).fold(acc)(acc + _)
        }
        Status(usable = reasons.isEmpty, reasons)
      case _ => Status(usable = true, Set.empty)
    }

  def seqNum: JValue = "sequenceNumber" -> DateTime.now(DateTimeZone.UTC).getMillis
}

object DocumentStatus extends v2.JsonSupport {
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
      case _ => Some(NoTitle)
    }
  }

  val AvailabilityChecker = Checker { doc =>
    doc \\ "available" match {
      case JBool(available) if !available => Some(Unavailable)
      case JObject(fields) =>
        val allAvailable = fields.forall {
          case (_, JBool(available)) => available
          case _ => false
        }
        if (!allAvailable) Some(Unavailable) else None
      case _ => None
    }
  }

  val SuppliableChecker = Checker { doc =>
    checkRights(
      doc \ "supplyRights" \ "WORLD",
      doc \ "supplyRights" \ "GB",
      doc \ "supplyRights" \ "ROW",
      Unsuppliable
    )
  }

  val SellableChecker = Checker { doc =>
    checkRights(
      doc \ "salesRights" \ "WORLD",
      doc \ "salesRights" \ "GB",
      doc \ "salesRights" \ "ROW",
      Unsellable
    )
  }

  val PublisherChecker = Checker { doc =>
    (doc \ "publisher", doc \ "imprint") match {
      case (JString(publisher), _) if publisher.nonEmpty => None
      case (_, JString(imprint)) if imprint.nonEmpty => None
      case _ => Some(NoPublisher)
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
        if (!hasCover) Some(NoCover) else None
      case _ => Some(NoCover)
    }
  }

  val EpubChecker = Checker { doc =>
    checkClassification(
      doc \ "media" \ "epubs" \ "best",
      doc \ "media" \ "epubs" \ "items" \\ "classification",
      NoEpub,
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
      case _ => Some(NotEnglish)
    }
  }

  val DescriptionChecker = Checker { doc =>
    checkClassification(
      doc \ "descriptions" \ "best",
      doc \ "descriptions" \ "items" \\ "classification",
      NoDescription,
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
        if (!usablePriceExists) Some(NoUsablePrice) else None
      case _ => Some(NoUsablePrice)
    }
  }

  val RacyTitleChecker = Checker { doc =>
    (doc \ "imprint", doc \ "publisher", doc \ "subjects") match {
      case (JString(imprint), JString(publisher), JArray(subjects)) if
        hasRestrictedImprint(RestrictedImprints, imprint) ||
        hasRestrictedPublisher(RestrictedPublishers, publisher) ||
        hasRestrictedSubject(RestrictedSubjects, subjects) => Some(Racy)
      case _ => None
    }
  }
}
