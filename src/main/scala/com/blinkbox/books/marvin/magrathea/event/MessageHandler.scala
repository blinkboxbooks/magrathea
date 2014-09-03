package com.blinkbox.books.marvin.magrathea.event

import java.io.IOException
import java.net.URL

import akka.actor.ActorRef
import akka.util.Timeout
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.event.Merger.Merge
import com.blinkbox.books.messaging.{ErrorHandler, Event, ReliableEventHandler}
import com.blinkbox.books.spray._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import spray.client.pipelining._
import spray.http.StatusCodes._
import spray.http.Uri.Path
import spray.http._
import spray.httpx.Json4sJacksonSupport

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, TimeoutException}

class MessageHandler(documentMerger: ActorRef, couchdbUrl: URL, bookSchema: String, contributorSchema: String,
                     errorHandler: ErrorHandler, retryInterval: FiniteDuration)
  extends ReliableEventHandler(errorHandler, retryInterval)
  with StrictLogging with Json4sJacksonSupport with JsonMethods {

  implicit val timeout = Timeout(retryInterval)
  implicit val json4sJacksonFormats = DefaultFormats

  private val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
  private val lookupUri = couchdbUrl.withPath(couchdbUrl.path ++ Path("/history/_design/index/_view/replace_lookup"))
  private val storeUri = couchdbUrl.withPath(couchdbUrl.path ++ Path("/history"))
  private val bookUri = couchdbUrl.withPath(couchdbUrl.path ++ Path("/history/_design/history/_view/book"))
  private val contributorUri = couchdbUrl.withPath(couchdbUrl.path ++ Path("/history/_design/history/_view/contributor"))

  override protected def handleEvent(event: Event, originalSender: ActorRef) = Future {
    val documentJson = parse(event.body.asString())
    val key = extractLookupKey(documentJson)

    logger.debug("Extracted lookup-key: {}", key)

    // check if the lookup-key exists
    lookupDocument(key).map { r =>
      val respJson = parse(r.entity.asString)
      val foundDocs = respJson \ "rows"
      // If there is at least on document with that key, delete all these documents except the first and
      // replace it with the received document's details. Otherwise, just store the received document.
      val finalJson = if (foundDocs.children.size > 0) {
        println(s"there are ${foundDocs.children.size} documents with this id")
        // TODO: delete if there are more documents
        //
        // Merging the value of the first row with the rest of the document. This will result in a document with
        // "_id" and "_rev" fields, which means replace the previous document with this new one.
        val idRev = ("_id" -> (respJson \ "rows")(0) \ "value" \ "_id") ~
                    ("_rev" -> (respJson \ "rows")(0) \ "value" \ "_rev")
        idRev merge documentJson
      } else {
        documentJson
      }
      // store the final document
      storeDocument(finalJson).map { r =>
        val status = r.status
        val resp = parse(r.entity.asString)
        val id = (resp \ "id").extract[String]
        val rev = (resp \ "rev").extract[String]
        if (status == Created) {
          logger.debug("Document stored with id: \"{}\", rev: \"{}\"", id, rev)
          val docSchema = (finalJson \ "$schema").extract[String]
          val docKey = compact(render(finalJson \ "classification"))
          fetchDocuments(docSchema, docKey).map { r =>
            val respJson = parse(r.entity.asString)
            val docs = (respJson \ "rows").children
            documentMerger ! Merge(docs)
          }
        } else {
          logger.error("An error occurred while storing document with id: \"{}\", rev: \"{}\"", id, rev)
        }
      }
    }
  }

  // Consider the error temporary if the exception or its root cause is an IO exception or timeout.
  @tailrec
  final override protected def isTemporaryFailure(e: Throwable) = e.isInstanceOf[IOException] ||
    e.isInstanceOf[TimeoutException] || Option(e.getCause).isDefined && isTemporaryFailure(e.getCause)

  private def extractLookupKey(json: JValue): String = {
    val schema = json \ "$schema"
    val remaining = (json \ "source" \ "$remaining")
      .removeField(_._1 == "processedAt").removeField(_._1 == "system")
    val classification = json \ "classification"
    compact(render(JArray(List(schema, remaining, classification))))
  }

  private def lookupDocument(key: String): Future[HttpResponse] = {
    pipeline(Get(lookupUri.withQuery(("key", key))))
  }

  private def storeDocument(document: JValue): Future[HttpResponse] = {
    pipeline(Post(storeUri, document))
  }

  private def fetchDocuments(schema: String, key: String): Future[HttpResponse] = {
    Future {
      if (schema == bookSchema) {
        bookUri.withQuery(("key", key))
      } else if (schema == contributorSchema) {
        contributorUri.withQuery(("key", key))
      } else {
        throw new IllegalArgumentException(s"Unsupported schema: $schema")
      }
    } flatMap { fetchUri => pipeline(Get(fetchUri)) }
  }
}
