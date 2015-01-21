package com.blinkbox.books.marvin.magrathea.api

import java.util.UUID
import java.util.concurrent.ForkJoinPool

import com.blinkbox.books.elasticsearch.client.ElasticClientApi._
import com.blinkbox.books.elasticsearch.client._
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.message._
import com.blinkbox.books.marvin.magrathea.{ElasticConfig, JsonDoc}
import com.blinkbox.books.spray.Page
import com.blinkbox.books.spray.v2.ListPage
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.source.DocumentSource
import com.typesafe.scalalogging.StrictLogging
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import spray.http.StatusCodes
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.{ExecutionContext, Future}

trait IndexService {
  def searchInCurrent(query: String, page: Page): Future[ListPage[JValue]]
  def searchInHistory(query: String, page: Page): Future[ListPage[JValue]]
  def indexCurrentDocument(docId: UUID, doc: JValue): Future[IndexResponse]
  def indexHistoryDocument(docId: UUID, doc: JValue): Future[IndexResponse]
  def deleteCurrentIndex(docIds: List[UUID]): Future[BulkResponse]
  def deleteHistoryIndex(docIds: List[UUID]): Future[BulkResponse]
  def reIndexCurrentDocument(docId: UUID, schema: String): Future[Boolean]
  def reIndexHistoryDocument(docId: UUID, schema: String): Future[Boolean]
  def reIndexCurrent(): Future[Unit]
  def reIndexHistory(): Future[Unit]
}

class DefaultIndexService(elasticClient: ElasticClient, config: ElasticConfig, documentDao: DocumentDao)
  extends IndexService with StrictLogging with Json4sJacksonSupport with JsonMethods {

  case class Json4sSource(root: JValue) extends DocumentSource {
    val json = compact(render(root))
  }

  private implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(new ForkJoinPool()))
  override implicit val json4sJacksonFormats = DefaultFormats ++ Formats.all

  override def searchInCurrent(queryText: String, page: Page): Future[ListPage[JValue]] =
    searchDocument(queryText, "current", page)

  override def searchInHistory(queryText: String, page: Page): Future[ListPage[JValue]] =
    searchDocument(queryText, "history", page)

  override def indexCurrentDocument(docId: UUID, doc: JValue): Future[IndexResponse] =
    indexDocument(docId, doc, "current")

  override def indexHistoryDocument(docId: UUID, doc: JValue): Future[IndexResponse] =
    indexDocument(docId, doc, "history")

  override def deleteHistoryIndex(docIds: List[UUID]): Future[BulkResponse] = deleteIndex(docIds, "history")

  override def deleteCurrentIndex(docIds: List[UUID]): Future[BulkResponse] = deleteIndex(docIds, "current")

  override def reIndexCurrentDocument(docId: UUID, schema: String): Future[Boolean] =
    reIndexDocument(docId, "current", schema)(documentDao.getCurrentDocumentById)

  override def reIndexHistoryDocument(docId: UUID, schema: String): Future[Boolean] =
    reIndexDocument(docId, "history", schema)(documentDao.getHistoryDocumentById)

  override def reIndexCurrent(): Future[Unit] =
    reIndexTable("current")(documentDao.countCurrentDocuments, documentDao.getCurrentDocuments)

  override def reIndexHistory(): Future[Unit] =
    reIndexTable("history")(documentDao.countHistoryDocuments, documentDao.getHistoryDocuments)

  private def searchDocument(queryText: String, docType: String, page: Page): Future[ListPage[JValue]] =
    elasticClient.execute {
      search in s"${config.index}/$docType" query queryText start page.offset limit page.count
    } map { resp =>
      val lastPage = (page.offset + page.count) >= resp.hits.total
      val hits = resp.hits.hits.map { hit =>
        val idField: JValue = "id" -> hit._id
        idField merge hit._source
      }.toList
      ListPage(hits, lastPage)
    }

  private def indexDocument(docId: UUID, doc: JValue, docType: String): Future[IndexResponse] =
    elasticClient.execute {
      index into s"${config.index}/$docType" doc Json4sSource(DocumentAnnotator.deAnnotate(doc)) id docId
    }

  private def deleteIndex(docIds: List[UUID], docType: String): Future[BulkResponse] =
    elasticClient.execute {
      bulk(
        docIds.map { docId =>
          delete id docId from s"${config.index}/$docType"
        }: _*
      )
    }

  private def reIndexDocument(docId: UUID, docType: String, schema: String)
    (getDocumentById: => (UUID, Option[String]) => Future[Option[JsonDoc]]): Future[Boolean] =
    getDocumentById(docId, Option(schema)).flatMap {
      case Some(doc) => indexDocument(docId, doc.toJson, docType).map(_ => true)
      case None => Future.successful(false)
    }

  private def reIndexTable(table: String)
    (count: => () => Future[Int], index: => (Int, Int) => Future[List[JsonDoc]]): Future[Unit] =
    for {
      totalDocs <- count()
      _ <- elasticClient.execute(delete index config.index)
      _ <- reIndexChunks(totalDocs, table, index)
    } yield ()
      
  private def reIndexChunks(totalDocs: Int, table: String, index: => (Int, Int) => Future[List[JsonDoc]]): Future[Unit] =
    (0 to totalDocs by config.reIndexChunks).foldLeft(Future.successful(())) { (acc, offset) =>
      acc.flatMap { _ =>
        index(config.reIndexChunks, offset).flatMap { docs =>
          indexBulkDocuments(docs, table).map(_ => ())
        }
      }
    }

  private def indexBulkDocuments(docs: List[JsonDoc], docType: String): Future[BulkResponse] =
    elasticClient.execute {
      bulk(
        docs.map { doc =>
          index into s"${config.index}/$docType" doc Json4sSource(DocumentAnnotator.deAnnotate(doc.toJson)) id doc.id
        }: _*
      )
    }
}
