package com.blinkbox.books.marvin.magrathea.message

import java.net.URL
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.SchemaConfig
import com.blinkbox.books.spray._
import com.typesafe.scalalogging.slf4j.StrictLogging
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
  def getLatestDocumentById(id: String): Future[Option[JValue]]
  def lookupHistoryDocument(key: String): Future[List[JValue]]
  def lookupLatestDocument(key: String): Future[List[JValue]]
  def fetchHistoryDocuments(schema: String, key: String): Future[List[JValue]]
  def storeHistoryDocument(document: JValue): Future[Unit]
  def storeLatestDocument(document: JValue): Future[String]
  def deleteHistoryDocuments(documents: List[(String, String)]): Future[Unit]
  def deleteLatestDocuments(documents: List[(String, String)]): Future[Unit]
}

class DefaultDocumentDao(couchDbUrl: URL, schemas: SchemaConfig)(implicit system: ActorSystem)
  extends DocumentDao with Json4sJacksonSupport with JsonMethods with StrictLogging {

  implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))
  implicit val json4sJacksonFormats = DefaultFormats
  private val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
  private val historyUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/history"))
  private val latestUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/latest"))
  private val lookupHistoryUri = historyUri.withPath(historyUri.path ++ Path("/_design/index/_view/replace_lookup"))
  private val lookupLatestUri = latestUri.withPath(latestUri.path ++ Path("/_design/index/_view/replace_lookup"))
  private val deleteHistoryUri = historyUri.withPath(historyUri.path ++ Path("/_bulk_docs"))
  private val deleteLatestUri = latestUri.withPath(latestUri.path ++ Path("/_bulk_docs"))
  private val bookUri = historyUri.withPath(historyUri.path ++ Path("/_design/history/_view/book"))
  private val contributorUri = historyUri.withPath(historyUri.path ++ Path("/_design/history/_view/contributor"))

  override def getLatestDocumentById(id: String): Future[Option[JValue]] = {
    pipeline(Get(latestUri.withPath(latestUri.path ++ Path(s"/$id")))).map {
      case resp if resp.status == OK => Option(parse(resp.entity.asString)
        .removeDirectField("_id").removeDirectField("_rev"))
      case resp if resp.status == NotFound => None
      case resp => throw new UnsuccessfulResponseException(resp)
    }
  }

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
    pipeline(Post(historyUri, document)).map {
      case resp if resp.status == Created => logger.debug("Stored history document")
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  override def storeLatestDocument(document: JValue): Future[String] =
    pipeline(Post(latestUri, document)).map {
      case resp if resp.status == Created =>
        val docId = (parse(resp.entity.asString) \ "id").extract[String]
        logger.info("Stored merged document with id: {}", docId)
        docId
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  override def deleteHistoryDocuments(documents: List[(String, String)]): Future[Unit] =
    getDeleteJson(documents).flatMap { json =>
      pipeline(Post(deleteHistoryUri, json)).map {
        case resp if resp.status == Created => logger.debug("Deleted history documents: {}", documents)
        case resp => throw new UnsuccessfulResponseException(resp)
      }
    }

  override def deleteLatestDocuments(documents: List[(String, String)]): Future[Unit] =
    getDeleteJson(documents).flatMap { json =>
      pipeline(Post(deleteLatestUri, json)).map {
        case resp if resp.status == Created => logger.debug("Deleted latest documents: {}", documents)
        case resp => throw new UnsuccessfulResponseException(resp)
      }
    }

  private def getHistoryFetchUri(schema: String, key: String): Future[Uri] = Future {
    if (schema == schemas.book) bookUri.withQuery(("key", key))
    else if (schema == schemas.contributor) contributorUri.withQuery(("key", key))
    else throw new IllegalArgumentException(s"Unsupported schema: $schema")
  }

  private def getDeleteJson(documents: List[(String, String)]): Future[JValue] = Future {
    "docs" -> documents.map { case (id, rev) =>
      ("_id" -> id) ~ ("_rev" -> rev) ~ ("_deleted" -> true)
    }
  }
}
