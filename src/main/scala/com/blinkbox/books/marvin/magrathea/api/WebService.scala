package com.blinkbox.books.marvin.magrathea.api

import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.AppConfig
import com.blinkbox.books.marvin.magrathea.message.DefaultDocumentDao
import com.blinkbox.books.spray.HealthCheckHttpService
import com.sksamuel.elastic4s.ElasticClient
import org.elasticsearch.common.settings.ImmutableSettings
import spray.http.Uri.Path
import spray.routing.HttpServiceActor

class WebService(config: AppConfig) extends HttpServiceActor {
  implicit val executionContext = DiagnosticExecutionContext(actorRefFactory.dispatcher)
  val documentDao = new DefaultDocumentDao(config.couchDbUrl, config.schemas)(context.system)
  val elasticSettings = ImmutableSettings.settingsBuilder().put("cluster.name", config.elasticsearch.cluster).build()
  val elasticClient = ElasticClient.remote(elasticSettings, (config.elasticsearch.host, config.elasticsearch.port))
  val searchService = new DefaultSearchService(elasticClient, config.elasticsearch)
  val restApi = new RestApi(config.service, documentDao, searchService)
  val healthService = new HealthCheckHttpService {
    override implicit def actorRefFactory = WebService.this.actorRefFactory
    override val basePath = Path("/")
  }

  def receive = runRoute(restApi.routes ~ healthService.routes)
}
