package com.blinkbox.books.marvin.magrathea.message

import java.net.URL
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.SchemaConfig
import com.blinkbox.books.spray._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import spray.client.pipelining._
import spray.http.StatusCodes._
import spray.http.Uri.Path
import spray.http.{HttpRequest, HttpResponse, Uri}
import spray.httpx.{Json4sJacksonSupport, UnsuccessfulResponseException}

import scala.concurrent.{ExecutionContext, Future}

trait DocumentDao {
  def lookupHistoryDocument(key: String): Future[List[JValue]]
  def lookupLatestDocument(key: String): Future[List[JValue]]
  def fetchHistoryDocuments(schema: String, key: String): Future[List[JValue]]
  def storeHistoryDocument(document: JValue): Future[Unit]
  def storeLatestDocument(document: JValue): Future[Unit]
  def deleteHistoryDocuments(documents: List[(String, String)]): Future[Unit]
  def deleteLatestDocuments(documents: List[(String, String)]): Future[Unit]
}

class DefaultDocumentDao(couchDbUrl: URL, config: SchemaConfig)
  extends DocumentDao with Json4sJacksonSupport with JsonMethods {

  implicit val system = ActorSystem("couchdb-system")
  implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))
  implicit val json4sJacksonFormats = DefaultFormats
  private val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
  private val lookupHistoryUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/history/_design/index/_view/replace_lookup"))
  private val lookupLatestUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/latest/_design/index/_view/replace_lookup"))
  private val storeHistoryUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/history"))
  private val storeLatestUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/latest"))
  private val deleteHistoryUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/history/_bulk_docs"))
  private val deleteLatestUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/latest/_bulk_docs"))
  private val bookUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/history/_design/history/_view/book"))
  private val contributorUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/history/_design/history/_view/contributor"))

  override def lookupHistoryDocument(key: String): Future[List[JValue]] =
    pipeline(Get(lookupHistoryUri.withQuery(("key", key)))).map {
      case resp if resp.status == OK => (parse(resp.entity.asString) \ "rows").children
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  override def lookupLatestDocument(key: String): Future[List[JValue]] =
    pipeline(Get(lookupLatestUri.withQuery(("key", key)))).map {
      case resp if resp.status == OK => (parse(resp.entity.asString) \ "rows").children
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  override def fetchHistoryDocuments(schema: String, key: String): Future[List[JValue]] =
    getHistoryFetchUri(schema, key).flatMap { uri =>
      pipeline(Get(uri)).map {
        case resp if resp.status == OK => (parse(resp.entity.asString) \ "rows").children.map(_ \ "value")
        case resp => throw new UnsuccessfulResponseException(resp)
      }
    }

  override def storeHistoryDocument(document: JValue): Future[Unit] =
    pipeline(Post(storeHistoryUri, document)).map {
      case resp if resp.status == Created => ()
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  override def storeLatestDocument(document: JValue): Future[Unit] =
    pipeline(Post(storeLatestUri, document)).map {
      case resp if resp.status == Created => ()
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  override def deleteHistoryDocuments(documents: List[(String, String)]): Future[Unit] =
    getDeleteJson(documents).flatMap { json =>
      pipeline(Post(deleteHistoryUri, json)).map {
        case resp if resp.status == Created => ()
        case resp => throw new UnsuccessfulResponseException(resp)
      }
    }

  override def deleteLatestDocuments(documents: List[(String, String)]): Future[Unit] =
    getDeleteJson(documents).flatMap { json =>
      pipeline(Post(deleteLatestUri, json)).map {
        case resp if resp.status == Created => ()
        case resp => throw new UnsuccessfulResponseException(resp)
      }
    }

  private def getHistoryFetchUri(schema: String, key: String): Future[Uri] = Future {
    if (schema == config.book) bookUri.withQuery(("key", key))
    else if (schema == config.contributor) contributorUri.withQuery(("key", key))
    else throw new IllegalArgumentException(s"Unsupported schema: $schema")
  }

  private def getDeleteJson(documents: List[(String, String)]): Future[JValue] = Future {
    "docs" -> documents.map { case (id, rev) =>
      ("_id" -> id) ~ ("_rev" -> rev) ~ ("_deleted" -> true)
    }
  }
}
