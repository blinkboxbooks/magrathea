package com.blinkbox.books.marvin.magrathea.message

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.{SchemaConfig, DistributorConfig}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.JsonAST.{JString, JValue}
import org.json4s.jackson.JsonMethods
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.Future

class DocumentDistributor(config: DistributorConfig, schemas: SchemaConfig)
  extends Json4sJacksonSupport with JsonMethods with StrictLogging {
  implicit val json4sJacksonFormats = DefaultFormats

  def sendDistributionInformation(document: JValue): Future[Unit] = Future {
    document \ "$schema" match {
      case JString(schema) if schema == schemas.book => ???
      case JString(schema) if schema == schemas.contributor => ???
      case x => throw new IllegalArgumentException(s"Cannot get distribution information from unsupported schema: $x")
    }
  }
}
