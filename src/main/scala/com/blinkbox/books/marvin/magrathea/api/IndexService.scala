package com.blinkbox.books.marvin.magrathea.api

import java.util.concurrent.Executors

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.ElasticConfig
import com.blinkbox.books.marvin.magrathea.message.{DocumentAnnotator, DocumentDao}
import com.blinkbox.books.spray.Page
import com.blinkbox.books.spray.v2.ListPage
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.source.DocumentSource
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexResponse
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.{ExecutionContext, Future}

trait IndexService {
  def searchInLatest(query: String, page: Page): Future[ListPage[JValue]]
  def searchInHistory(query: String, page: Page): Future[ListPage[JValue]]
  def indexLatestDocument(doc: JValue, docId: String): Future[IndexResponse]
  def indexHistoryDocument(doc: JValue, docId: String): Future[IndexResponse]
  def reIndexLatestDocument(docId: String, schema: String): Future[Boolean]
  def reIndexHistoryDocument(docId: String, schema: String): Future[Boolean]
  def reIndexLatest(): Future[Unit]
  def reIndexHistory(): Future[Unit]
}

class DefaultIndexService(elasticClient: ElasticClient, config: ElasticConfig, documentDao: DocumentDao)
  extends IndexService with StrictLogging with Json4sJacksonSupport with JsonMethods {

  class Json4sSource(root: JValue) extends DocumentSource {
    def json = compact(render(root))
  }

  object Json4sSource {
    def apply(root: JValue) = new Json4sSource(root)
  }

  implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))
  override implicit def json4sJacksonFormats = DefaultFormats

  elasticClient.execute { create index config.index }

  override def searchInLatest(queryText: String, page: Page): Future[ListPage[JValue]] =
    searchDocument(queryText, "latest", page)

  override def searchInHistory(queryText: String, page: Page): Future[ListPage[JValue]] =
    searchDocument(queryText, "history", page)

  override def indexLatestDocument(doc: JValue, docId: String): Future[IndexResponse] =
    indexDocument(doc, docId, "latest")

  override def indexHistoryDocument(doc: JValue, docId: String): Future[IndexResponse] =
    indexDocument(doc, docId, "history")

  override def reIndexLatestDocument(docId: String, schema: String): Future[Boolean] =
    reIndexDocument(docId, "latest", schema)(documentDao.getLatestDocumentById)

  override def reIndexHistoryDocument(docId: String, schema: String): Future[Boolean] =
    reIndexDocument(docId, "history", schema)(documentDao.getHistoryDocumentById)

  override def reIndexLatest(): Future[Unit] =
    reIndexTable("latest")(documentDao.countLatestDocuments)(documentDao.getLatestDocuments)

  override def reIndexHistory(): Future[Unit] =
    reIndexTable("history")(documentDao.countHistoryDocuments)(documentDao.getHistoryDocuments)

  private def searchDocument(queryText: String, docType: String, page: Page): Future[ListPage[JValue]] =
    elasticClient.execute {
      search in s"${config.index}/$docType" query queryText start page.offset limit page.count
    } map { resp =>
      val lastPage = (page.offset + page.count) >= resp.getHits.totalHits()
      val hits = resp.getHits.hits().map(hit => parse(hit.getSourceAsString)).toList
      ListPage(hits, lastPage)
    }

  private def indexDocument(doc: JValue, docId: String, docType: String): Future[IndexResponse] =
    elasticClient.execute {
      index into s"${config.index}/$docType" doc Json4sSource(deAnnotated(doc)) id docId
    }

  private def reIndexDocument(docId: String, docType: String, schema: String)
    (getDocumentById: => (String, Option[String]) => Future[Option[JValue]]): Future[Boolean] =
    getDocumentById(docId, Option(schema)).flatMap {
      case Some(doc) => indexDocument(doc, docId, docType).map(_ => true)
      case None => Future.successful(false)
    }

  private def reIndexTable(table: String)(count: => () => Future[Int])(index: => (Int, Int) => Future[List[JValue]]): Future[Unit] =
    count().flatMap { totalDocs =>
      (0 to totalDocs by config.reIndexChunks).foldLeft(Future.successful(())) { (acc, offset) =>
        acc.flatMap { _ =>
          index(config.reIndexChunks, offset).flatMap { docs =>
            indexBulkDocuments(docs, table).map(_ => ())
          }
        }
      }
    }

  private def indexBulkDocuments(docs: List[JValue], docType: String): Future[BulkResponse] =
    elasticClient.execute {
      bulk(
        docs.map { doc =>
          index into s"${config.index}/$docType" doc Json4sSource(deAnnotated(doc)) id (doc \ "_id").extract[String]
        }: _*
      )
    }

  private def deAnnotated(doc: JValue): JValue = DocumentAnnotator.deAnnotate(clean(doc))

  private def clean(doc: JValue): JValue = doc.removeDirectField("_id").removeDirectField("_rev")
}
