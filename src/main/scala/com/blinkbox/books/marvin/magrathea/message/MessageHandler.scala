package com.blinkbox.books.marvin.magrathea.message

import java.io.IOException

import akka.actor.ActorRef
import akka.util.Timeout
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.Json4sExtensions._
import com.blinkbox.books.messaging.{ErrorHandler, Event, ReliableEventHandler}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.JsonAST.{JArray, JNothing, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import spray.can.Http.ConnectionException
import spray.httpx.Json4sJacksonSupport

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, TimeoutException}
import scala.language.{implicitConversions, postfixOps}

class MessageHandler(documentDao: DocumentDao, errorHandler: ErrorHandler, retryInterval: FiniteDuration)
                    (documentMerge: (JValue, JValue) => JValue) extends ReliableEventHandler(errorHandler, retryInterval)
  with StrictLogging with Json4sJacksonSupport with JsonMethods {

  implicit val timeout = Timeout(retryInterval)
  implicit val json4sJacksonFormats = DefaultFormats

  override protected def handleEvent(event: Event, originalSender: ActorRef) = for {
    incomingDoc <- Future(parse(event.body.asString()))
    lookupKey = extractLookupKey(incomingDoc)
    lookupKeyMatches <- documentDao.lookupDocument(lookupKey)
    normalisedIncomingDoc = normaliseDocument(incomingDoc, lookupKeyMatches)
    _ <- normaliseDatabase(lookupKeyMatches)
    _ <- documentDao.storeHistoryDocument(normalisedIncomingDoc)
    (schema, classification) = extractSchemaAndClassification(normalisedIncomingDoc)
    history <- documentDao.fetchHistoryDocuments(schema, classification)
    mergedDoc = mergeDocuments(history)
    _ <- documentDao.storeLatestDocument(mergedDoc)
  } yield ()

  // Consider the error temporary if the exception or its root cause is an IO exception, timeout or connection exception.
  @tailrec
  final override protected def isTemporaryFailure(e: Throwable) = e.isInstanceOf[IOException] ||
    e.isInstanceOf[TimeoutException] || e.isInstanceOf[ConnectionException] ||
    Option(e.getCause).isDefined && isTemporaryFailure(e.getCause)

  private def mergeDocuments(documents: List[JValue]): JValue = {
    if (documents.isEmpty)
      throw new IllegalArgumentException(s"Expected to merge a non-empty history list")
    val merged = documents.par.reduce(documentMerge)
    merged.removeDirectField("_id").removeDirectField("_rev")
  }

  private def normaliseDocument(document: JValue, lookupKeyMatches: List[JValue]): JValue =
    // Merging the value of the first row with the rest of the document. This will result in a document with
    // "_id" and "_rev" fields, which means replace the previous document with this new one.
    // If the first row does not exist, the document will remain unchanged.
    lookupKeyMatches.headOption match {
      case Some(item) =>
        val id = item \ "value" \ "_id"
        val rev = item \ "value" \ "_rev"
        if (id == JNothing || rev == JNothing)
          throw new IllegalArgumentException(s"Cannot extract _id and _rev: ${compact(render(item))}")
        document merge (("_id" -> id.extract[String]) ~ ("_rev" -> rev.extract[String]))
      case None => document
    }

  private def normaliseDatabase(lookupKeyMatches: List[JValue]): Future[Unit] = {
    // If there is at least on document with that key, delete all these documents except the first
    if (lookupKeyMatches.size > 1) {
      val deleteDocuments = lookupKeyMatches.drop(1).map { item =>
        val id = item \ "value" \ "_id"
        val rev = item \ "value" \ "_rev"
        if (id == JNothing || rev == JNothing)
          throw new IllegalArgumentException(s"Cannot extract _id and _rev: ${compact(render(item))}")
        (id.extract[String], rev.extract[String])
      }
      documentDao.deleteDocuments(deleteDocuments)
    } else Future.successful(())
  }

  private def extractLookupKey(document: JValue): String = {
    val schema = document \ "$schema"
    val remaining = (document \ "source" \ "$remaining")
      .removeDirectField("processedAt").removeDirectField("system")
    val classification = document \ "classification"
    if (schema == JNothing || remaining == JNothing || classification == JNothing)
      throw new IllegalArgumentException(
        s"Cannot extract lookup key (schema, remaining, classification): ${compact(render(document))}")
    compact(render(JArray(List(schema, remaining, classification))))
  }

  private def extractSchemaAndClassification(document: JValue): (String, String) = {
    val schema = document \ "$schema"
    val classification = document \ "classification"
    if (schema == JNothing || classification == JNothing)
      throw new IllegalArgumentException(s"Cannot extract schema and classification: ${compact(render(document))}")
    (schema.extract[String], compact(render(classification)))
  }
}
