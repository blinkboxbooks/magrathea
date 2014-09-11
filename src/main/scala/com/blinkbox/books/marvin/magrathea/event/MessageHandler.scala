package com.blinkbox.books.marvin.magrathea.event

import java.io.IOException
import java.net.URL

import akka.actor.ActorRef
import akka.pattern.ask
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
import spray.httpx.{Json4sJacksonSupport, UnsuccessfulResponseException}

import scala.annotation.tailrec
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Future, TimeoutException}
import scala.language.postfixOps

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

  override protected def handleEvent(event: Event, originalSender: ActorRef) = for {
    incomingDoc <- Future(parse(event.body.asString()))
    finalDoc <- lookupDocument(incomingDoc)
    _ <- storeDocumentRequest(finalDoc)
    (schema, key) <- extractSchemaAndKey(finalDoc)
    docs <- fetchDocuments(schema, key)
    _ <- documentMerger.ask(Merge(docs))(Timeout(5 seconds))
  } yield ()

  // Consider the error temporary if the exception or its root cause is an IO exception or timeout.
  @tailrec
  final override protected def isTemporaryFailure(e: Throwable) = e.isInstanceOf[IOException] ||
    e.isInstanceOf[TimeoutException] || Option(e.getCause).isDefined && isTemporaryFailure(e.getCause)

  private def lookupDocument(documentJson: JValue): Future[JValue] = for {
    key <- extractLookupKey(documentJson)
    resp <- lookupDocumentRequest(key)
  } yield {
    val foundDocs = resp \ "rows"
    // If there is at least on document with that key, delete all these documents except the first and
    // replace it with the received document's details. Otherwise, just store the received document.
    if (foundDocs.children.size > 0) {
      println(s"there are ${foundDocs.children.size} documents with this id")
      // TODO: delete if there are more documents
      //
      // Merging the value of the first row with the rest of the document. This will result in a document with
      // "_id" and "_rev" fields, which means replace the previous document with this new one.
      documentJson merge ("_id" -> (resp \ "rows")(0) \ "value" \ "_id") ~
                         ("_rev" -> (resp \ "rows")(0) \ "value" \ "_rev")
    } else {
      documentJson
    }
  }

  private def extractLookupKey(document: JValue): Future[String] = Future {
    val schema = document \ "$schema"
    val remaining = (document \ "source" \ "$remaining")
      .removeField(_._1 == "processedAt").removeField(_._1 == "system")
    val classification = document \ "classification"
    compact(render(JArray(List(schema, remaining, classification))))
  }

  private def extractSchemaAndKey(document: JValue): Future[(String, String)] = Future {
    val schema = (document \ "$schema").extract[String]
    val key = compact(render(document \ "classification"))
    (schema, key)
  }

  private def fetchDocuments(schema: String, key: String): Future[List[JValue]] = Future {
    if (schema == bookSchema) bookUri.withQuery(("key", key))
    else if (schema == contributorSchema) contributorUri.withQuery(("key", key))
    else throw new IllegalArgumentException(s"Unsupported schema: $schema")
  } flatMap { fetchUri => fetchDocumentsRequest(fetchUri) } map { d => (d \ "rows").children }

  private def lookupDocumentRequest(key: String): Future[JValue] = {
    pipeline(Get(lookupUri.withQuery(("key", key)))).map {
      case resp if resp.status == OK => parse(resp.entity.asString)
      case resp => throw new UnsuccessfulResponseException(resp)
    }
  }

  private def storeDocumentRequest(document: JValue): Future[JValue] = {
    pipeline(Post(storeUri, document)).map {
      case resp if resp.status == Created => parse(resp.entity.asString)
      case resp => throw new UnsuccessfulResponseException(resp)
    }
  }

  private def fetchDocumentsRequest(uri: Uri): Future[JValue] = {
    pipeline(Get(uri)).map {
      case resp if resp.status == OK => parse(resp.entity.asString)
      case resp => throw new UnsuccessfulResponseException(resp)
    }
  }
}
