package com.blinkbox.books.marvin.magrathea.message

import java.net.URL
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import com.blinkbox.books.config.DatabaseConfig
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.SchemaConfig
import com.blinkbox.books.spray._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.Formats
import org.json4s.JsonAST.{JString, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import spray.client.pipelining._
import spray.http.StatusCodes._
import spray.http.Uri.Path
import spray.http.{HttpRequest, HttpResponse, Uri}
import spray.httpx.{Json4sJacksonSupport, UnsuccessfulResponseException}

import scala.concurrent.{ExecutionContext, Future}

trait DocumentDao {
  def getLatestDocumentById(id: String, schema: Option[String] = None): Future[Option[JValue]]
  def getHistoryDocumentById(id: String, schema: Option[String] = None): Future[Option[JValue]]
  def getLatestDocumentCount(): Future[Int]
  def getHistoryDocumentCount(): Future[Int]
  def getAllLatestDocuments(count: Int, offset: Int): Future[List[JValue]]
  def getAllHistoryDocuments(count: Int, offset: Int): Future[List[JValue]]
  def lookupLatestDocument(key: String): Future[List[JValue]]
  def lookupHistoryDocument(key: String): Future[List[JValue]]
  def fetchHistoryDocuments(schema: String, key: String): Future[List[JValue]]
  def storeLatestDocument(document: JValue): Future[String]
  def storeHistoryDocument(document: JValue): Future[Unit]
  def deleteLatestDocuments(documents: List[(String, String)]): Future[Unit]
  def deleteHistoryDocuments(documents: List[(String, String)]): Future[Unit]
}

class PostgresDocumentDao(config: DatabaseConfig) extends DocumentDao
  with Json4sJacksonSupport with JsonMethods with StrictLogging {

  override implicit val json4sJacksonFormats = DefaultFormats

  override def getLatestDocumentById(id: String, schema: Option[String]): Future[Option[JValue]] = ???

  override def getHistoryDocumentById(id: String, schema: Option[String]): Future[Option[JValue]] = ???

  override def getAllLatestIds(): Future[List[String]] = ???

  override def getAllHistoryIds(): Future[List[String]] = ???

  override def lookupLatestDocument(key: String): Future[List[JValue]] = ???

  override def lookupHistoryDocument(key: String): Future[List[JValue]] = ???

  override def fetchHistoryDocuments(schema: String, key: String): Future[List[JValue]] = ???

  override def storeLatestDocument(document: JValue): Future[String] = ???

  override def storeHistoryDocument(document: JValue): Future[Unit] = ???

  override def deleteLatestDocuments(documents: List[(String, String)]): Future[Unit] = ???

  override def deleteHistoryDocuments(documents: List[(String, String)]): Future[Unit] = ???
}

class CouchDocumentDao(couchDbUrl: URL, schemas: SchemaConfig)(implicit system: ActorSystem)
  extends DocumentDao with Json4sJacksonSupport with JsonMethods with StrictLogging {

  override implicit val json4sJacksonFormats = DefaultFormats
  implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))
  private val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
  private val historyUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/history"))
  private val latestUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/latest"))
  private val lookupHistoryUri = historyUri.withPath(historyUri.path ++ Path("/_design/index/_view/replace_lookup"))
  private val lookupLatestUri = latestUri.withPath(latestUri.path ++ Path("/_design/index/_view/replace_lookup"))
  private val deleteHistoryUri = historyUri.withPath(historyUri.path ++ Path("/_bulk_docs"))
  private val deleteLatestUri = latestUri.withPath(latestUri.path ++ Path("/_bulk_docs"))
  private val bookUri = historyUri.withPath(historyUri.path ++ Path("/_design/history/_view/book"))
  private val contributorUri = historyUri.withPath(historyUri.path ++ Path("/_design/history/_view/contributor"))

  override def getLatestDocumentById(id: String, schema: Option[String] = None): Future[Option[JValue]] =
    getDocumentById(id, schema, latestUri)

  override def getHistoryDocumentById(id: String, schema: Option[String] = None): Future[Option[JValue]] =
    getDocumentById(id, schema, historyUri)

  override def getLatestDocumentCount(): Future[Int] = getTableDocumentCount(latestUri)

  override def getHistoryDocumentCount(): Future[Int] = getTableDocumentCount(historyUri)

  override def getAllLatestDocuments(count: Int, offset: Int): Future[List[JValue]] =
    getAllDocsFromDatabase(latestUri, count, offset)

  override def getAllHistoryDocuments(count: Int, offset: Int): Future[List[JValue]] =
    getAllDocsFromDatabase(historyUri, count, offset)

  override def lookupLatestDocument(key: String): Future[List[JValue]] = lookupDocument(key, lookupLatestUri)

  override def lookupHistoryDocument(key: String): Future[List[JValue]] = lookupDocument(key, lookupHistoryUri)

  override def fetchHistoryDocuments(schema: String, key: String): Future[List[JValue]] =
    getHistoryFetchUri(schema, key).flatMap { uri =>
      pipeline(Get(uri)).map {
        case resp if resp.status == OK => (parse(resp.entity.asString) \ "rows").children.map(_ \ "value")
        case resp => throw new UnsuccessfulResponseException(resp)
      }
    }

  override def storeLatestDocument(document: JValue): Future[String] =
    storeDocument(document, latestUri) { data =>
      val docId = (parse(data) \ "id").extract[String]
      logger.info("Stored merged document with id: {}", docId)
      docId
    }

  override def storeHistoryDocument(document: JValue): Future[Unit] =
    storeDocument(document, historyUri) { data =>
      logger.debug("Stored history document")
    }

  override def deleteLatestDocuments(documents: List[(String, String)]): Future[Unit] =
    deleteDocuments(documents, deleteLatestUri) {
      logger.debug("Deleted latest documents: {}", documents)
    }

  override def deleteHistoryDocuments(documents: List[(String, String)]): Future[Unit] =
    deleteDocuments(documents, deleteHistoryUri) {
      logger.debug("Deleted history documents: {}", documents)
    }

  private def getTableDocumentCount(uri: Uri): Future[Int] =
    pipeline(Get(uri)).map {
      case resp if resp.status == OK => (parse(resp.entity.asString) \ "doc_count").extract[Int]
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def getHistoryFetchUri(schema: String, key: String): Future[Uri] = Future {
    if (schema == schemas.book) bookUri.withQuery(("key", key))
    else if (schema == schemas.contributor) contributorUri.withQuery(("key", key))
    else throw new IllegalArgumentException(s"Unsupported schema: $schema")
  }

  private def getDocumentById(id: String, schema: Option[String] = None, uri: Uri): Future[Option[JValue]] =
    pipeline(Get(uri.withPath(uri.path ++ Path(s"/$id")))).map {
      case resp if resp.status == OK =>
        val doc = parse(resp.entity.asString).removeDirectField("_id").removeDirectField("_rev")
        schema match {
          case Some(s) if doc \ "$schema" == JString(s) => Some(doc)
          case None => Some(doc)
          case _ => None
        }
      case resp if resp.status == NotFound => None
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def getAllDocsFromDatabase(uri: Uri, count: Int, offset: Int): Future[List[JValue]] =
    pipeline(Get(uri.withPath(uri.path ++ Path("/_all_docs"))
      .withQuery(("limit", count.toString), ("skip", offset.toString), ("include_docs", "true")))).map {
      case resp if resp.status == OK => (parse(resp.entity.asString) \ "rows").children.map(_ \ "doc")
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def lookupDocument(key: String, uri: Uri): Future[List[JValue]] =
    pipeline(Get(uri.withQuery(("key", key)))).map {
      case resp if resp.status == OK => (parse(resp.entity.asString) \ "rows").children
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def storeDocument[T](document: JValue, uri: Uri)(f: => String => T): Future[T] =
    pipeline(Post(uri, document)).map {
      case resp if resp.status == Created => f(resp.entity.asString)
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def getDeleteJson(documents: List[(String, String)]): Future[JValue] = Future {
    "docs" -> documents.map { case (id, rev) =>
      ("_id" -> id) ~ ("_rev" -> rev) ~ ("_deleted" -> true)
    }
  }

  private def deleteDocuments(documents: List[(String, String)], uri: Uri)(f: => Unit): Future[Unit] =
    getDeleteJson(documents).flatMap { json =>
      pipeline(Post(uri, json)).map {
        case resp if resp.status == Created => f
        case resp => throw new UnsuccessfulResponseException(resp)
      }
    }
}
