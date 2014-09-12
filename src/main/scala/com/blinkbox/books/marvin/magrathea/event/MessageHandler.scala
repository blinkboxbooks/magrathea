package com.blinkbox.books.marvin.magrathea.event

import java.io.IOException
import java.net.URL

import akka.actor.ActorRef
import akka.util.Timeout
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.SchemaConfig
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
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, TimeoutException}
import scala.language.postfixOps

class MessageHandler(couchdbUrl: URL, schemaConfig: SchemaConfig, errorHandler: ErrorHandler, retryInterval: FiniteDuration)
  extends ReliableEventHandler(errorHandler, retryInterval)
  with StrictLogging with Json4sJacksonSupport with JsonMethods {

  implicit val timeout = Timeout(retryInterval)
  implicit val json4sJacksonFormats = DefaultFormats

  private val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
  private val lookupUri = couchdbUrl.withPath(couchdbUrl.path ++ Path("/history/_design/index/_view/replace_lookup"))
  private val storeHistoryUri = couchdbUrl.withPath(couchdbUrl.path ++ Path("/history"))
  private val storeCurrentUri = couchdbUrl.withPath(couchdbUrl.path ++ Path("/current"))
  private val deleteUri = couchdbUrl.withPath(couchdbUrl.path ++ Path("/history/_bulk_docs"))
  private val bookUri = couchdbUrl.withPath(couchdbUrl.path ++ Path("/history/_design/history/_view/book"))
  private val contributorUri = couchdbUrl.withPath(couchdbUrl.path ++ Path("/history/_design/history/_view/contributor"))

  override protected def handleEvent(event: Event, originalSender: ActorRef) = for {
    incomingDoc <- Future(parse(event.body.asString()))
    lookupKey <- extractLookupKey(incomingDoc)
    lookupResult <- lookupDocumentRequest(lookupKey)
    historyDocument <- normaliseDocument(incomingDoc, lookupResult)
    _ <- normaliseDatabase(lookupResult)
    _ <- storeHistoryDocumentRequest(historyDocument)
    (schema, key) <- extractSchemaAndKey(historyDocument)
    docsList <- fetchDocuments(schema, key)
    mergedDoc <- mergeDocuments(docsList)
    _ <- storeCurrentDocumentRequest(mergedDoc)
  } yield ()

  // Consider the error temporary if the exception or its root cause is an IO exception or timeout.
  @tailrec
  final override protected def isTemporaryFailure(e: Throwable) = e.isInstanceOf[IOException] ||
    e.isInstanceOf[TimeoutException] || Option(e.getCause).isDefined && isTemporaryFailure(e.getCause)

  private def mergeDocuments(documents: List[JValue]): Future[JValue] = Future {
    val merged = documents.par.reduceLeft(DocumentMerger.merge)
    merged.removeField(_._1 == "_id").removeField(_._1 == "_rev")
  }

  private def normaliseDocument(document: JValue, lookupResult: JValue): Future[JValue] = Future {
    // Merging the value of the first row with the rest of the document. This will result in a document with
    // "_id" and "_rev" fields, which means replace the previous document with this new one.
    // If the first row does not exist, the document will remain unchanged.
    document merge ("_id" -> (lookupResult \ "rows")(0) \ "value" \ "_id") ~
                   ("_rev" -> (lookupResult \ "rows")(0) \ "value" \ "_rev")
  }

  private def normaliseDatabase(lookupResult: JValue): Future[Unit] = Future {
    // If there is at least on document with that key, delete all these documents except the first
    if ((lookupResult \ "rows").children.size > 1) {
      val deleteJson: JValue = "docs" -> (lookupResult \ "rows").children.drop(1).map { d =>
        ("_id" -> d \ "value" \ "_id") ~ ("_rev" -> d \ "value" \ "_rev") ~ ("_deleted" -> true)
      }
      deleteDocumentsRequest(deleteJson)
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
    if (schema == schemaConfig.book) bookUri.withQuery(("key", key))
    else if (schema == schemaConfig.contributor) contributorUri.withQuery(("key", key))
    else throw new IllegalArgumentException(s"Unsupported schema: $schema")
  } flatMap { uri => fetchDocumentsRequest(uri).map(d => (d \ "rows").children.map(_ \ "value")) }

  private def lookupDocumentRequest(key: String): Future[JValue] =
    pipeline(Get(lookupUri.withQuery(("key", key)))).map {
      case resp if resp.status == OK => parse(resp.entity.asString)
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def storeHistoryDocumentRequest(document: JValue): Future[Unit] =
    pipeline(Post(storeHistoryUri, document)).map {
      case resp if resp.status == Created => ()
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def storeCurrentDocumentRequest(document: JValue): Future[Unit] =
    pipeline(Post(storeCurrentUri, document)).map {
      case resp if resp.status == Created => ()
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def deleteDocumentsRequest(document: JValue): Future[Unit] =
    pipeline(Post(deleteUri, document)).map {
      case resp if resp.status == Created => ()
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def fetchDocumentsRequest(uri: Uri): Future[JValue] =
    pipeline(Get(uri)).map {
      case resp if resp.status == OK => parse(resp.entity.asString)
      case resp => throw new UnsuccessfulResponseException(resp)
    }
}
