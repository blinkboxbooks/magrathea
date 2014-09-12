package com.blinkbox.books.marvin.magrathea.event

import java.io.IOException

import akka.actor.ActorRef
import akka.util.Timeout
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.messaging.{ErrorHandler, Event, ReliableEventHandler}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import spray.can.Http.ConnectionException
import spray.httpx.Json4sJacksonSupport

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, TimeoutException}
import scala.language.postfixOps

class MessageHandler(eventDao: EventDao, errorHandler: ErrorHandler, retryInterval: FiniteDuration)
  extends ReliableEventHandler(errorHandler, retryInterval) with StrictLogging with Json4sJacksonSupport with JsonMethods {

  implicit val timeout = Timeout(retryInterval)
  implicit val json4sJacksonFormats = DefaultFormats

  override protected def handleEvent(event: Event, originalSender: ActorRef) = for {
    incomingDoc <- Future(parse(event.body.asString()))
    lookupKey = extractLookupKey(incomingDoc)
    lookupResult <- eventDao.lookupDocument(lookupKey)
    historyDocument = normaliseDocument(incomingDoc, lookupResult)
    _ <- normaliseDatabase(lookupResult)
    _ <- eventDao.storeHistoryDocument(historyDocument)
    (schema, key) = extractSchemaAndKey(historyDocument)
    docsList <- eventDao.fetchHistoryDocuments(schema, key)
    mergedDoc = mergeDocuments(docsList)
    _ <- eventDao.storeCurrentDocument(mergedDoc)
  } yield ()

  // Consider the error temporary if the exception or its root cause is an IO exception or timeout.
  @tailrec
  final override protected def isTemporaryFailure(e: Throwable) = e.isInstanceOf[IOException] ||
    e.isInstanceOf[TimeoutException] || e.isInstanceOf[ConnectionException] ||
    Option(e.getCause).isDefined && isTemporaryFailure(e.getCause)

  private def mergeDocuments(documents: List[JValue]): JValue = {
    val merged = documents.par.reduceLeft(DocumentMerger.merge)
    merged.removeField(_._1 == "_id").removeField(_._1 == "_rev")
  }

  private def normaliseDocument(document: JValue, lookupResult: JValue): JValue =
    // Merging the value of the first row with the rest of the document. This will result in a document with
    // "_id" and "_rev" fields, which means replace the previous document with this new one.
    // If the first row does not exist, the document will remain unchanged.
    if ((lookupResult \ "rows").children.size > 0)
      document merge ("_id" -> (lookupResult \ "rows")(0) \ "value" \ "_id") ~
                     ("_rev" -> (lookupResult \ "rows")(0) \ "value" \ "_rev")
    else document

  private def normaliseDatabase(lookupResult: JValue): Future[Unit] = {
    // If there is at least on document with that key, delete all these documents except the first
    if ((lookupResult \ "rows").children.size > 1) {
      val deleteDocuments = "docs" -> (lookupResult \ "rows").children.drop(1).map { d =>
        ("_id" -> d \ "value" \ "_id") ~ ("_rev" -> d \ "value" \ "_rev") ~ ("_deleted" -> true)
      }
      eventDao.deleteDocuments(deleteDocuments)
    } else Future.successful(())
  }

  private def extractLookupKey(document: JValue): String = {
    val schema = document \ "$schema"
    val remaining = (document \ "source" \ "$remaining")
      .removeField(_._1 == "processedAt").removeField(_._1 == "system")
    val classification = document \ "classification"
    compact(render(JArray(List(schema, remaining, classification))))
  }

  private def extractSchemaAndKey(document: JValue): (String, String) = {
    val schema = (document \ "$schema").extract[String]
    val key = compact(render(document \ "classification"))
    (schema, key)
  }
}
