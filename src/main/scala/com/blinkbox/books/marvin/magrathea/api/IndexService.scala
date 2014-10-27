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
import org.elasticsearch.action.index.IndexResponse
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.{ExecutionContext, Future}

trait IndexService {
  def searchByQuery(query: String)(page: Page): Future[ListPage[JValue]]
  def indexDocument(doc: JValue, docId: String): Future[IndexResponse]
  def reIndexDocument(docId: String, schema: String): Future[Boolean]
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

  override def searchByQuery(queryText: String)(page: Page): Future[ListPage[JValue]] =
    elasticClient.execute {
      search in s"${config.index}/latest" query queryText start page.offset limit page.count
    } map { resp =>
      val lastPage = (page.offset + page.count) >= resp.getHits.totalHits()
      val hits = resp.getHits.hits().map(hit => parse(hit.getSourceAsString)).toList
      ListPage(hits, lastPage)
    }

  override def indexDocument(doc: JValue, docId: String): Future[IndexResponse] = elasticClient.execute {
    index into s"${config.index}/latest" doc Json4sSource(doc) id docId
  }

  override def reIndexDocument(docId: String, schema: String): Future[Boolean] =
    documentDao.getLatestDocumentById(docId, Option(schema)).flatMap {
      case Some(doc) => indexDocument(deAnnotated(doc), docId).map(_ => true)
      case None => Future.successful(false)
    }

  override def reIndexLatest(): Future[Unit] = ???

  override def reIndexHistory(): Future[Unit] = ???

  private def deAnnotated(doc: JValue): JValue =
    DocumentAnnotator.deAnnotate(doc).removeDirectField("_id").removeDirectField("_rev")

}
