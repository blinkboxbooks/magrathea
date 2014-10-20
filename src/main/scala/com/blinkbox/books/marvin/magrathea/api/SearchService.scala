package com.blinkbox.books.marvin.magrathea.api

import java.util.concurrent.Executors

import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.ElasticConfig
import com.blinkbox.books.spray.Page
import com.blinkbox.books.spray.v2.ListPage
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.{ExecutionContext, Future}

trait SearchService {
  def searchByQuery(query: String)(page: Page): Future[ListPage[JValue]]
}

class DefaultSearchService(elasticClient: ElasticClient, elasticConfig: ElasticConfig) extends SearchService
  with StrictLogging with Json4sJacksonSupport with JsonMethods {

  implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))
  override implicit def json4sJacksonFormats = DefaultFormats

  override def searchByQuery(queryText: String)(page: Page): Future[ListPage[JValue]] =
    elasticClient.execute {
      search in s"${elasticConfig.index}/latest" query queryText start page.offset limit page.count
    } map { resp =>
      val lastPage = (page.offset + page.count) >= resp.getHits.totalHits()
      val hits = resp.getHits.hits().map(hit => parse(hit.getSourceAsString)).toList
      ListPage(hits, lastPage)
    }
}
