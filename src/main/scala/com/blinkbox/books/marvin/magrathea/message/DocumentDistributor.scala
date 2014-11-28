package com.blinkbox.books.marvin.magrathea.message

import java.util.concurrent.Executors

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.message.DocumentDistributor.Reason.Reason
import com.blinkbox.books.marvin.magrathea.{DistributorConfig, SchemaConfig}
import com.typesafe.scalalogging.StrictLogging
import org.json4s.JsonAST.{JString, JValue}
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
  implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))
  implicit val json4sJacksonFormats = DefaultFormats

  /** TODO implement this */
  def sendDistributionInformation(document: JValue): Future[Unit] = Future {
    document \ "$schema" match {
      case JString(schema) if schema == schemas.book => ()
      case JString(schema) if schema == schemas.contributor => ()
      case x => throw new IllegalArgumentException(s"Cannot get distribution information from unsupported schema: $x")
    }
  }
}
