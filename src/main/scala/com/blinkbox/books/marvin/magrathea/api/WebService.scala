package com.blinkbox.books.marvin.magrathea.api

import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.AppConfig
import com.blinkbox.books.spray.HealthCheckHttpService
import spray.http.Uri.Path
import spray.routing.HttpServiceActor

class WebService(config: AppConfig) extends HttpServiceActor {
  val that = this
  implicit val executionContext = DiagnosticExecutionContext(actorRefFactory.dispatcher)
  val restApi = new RestApi(config.service)
  val healthService = new HealthCheckHttpService {
    override implicit def actorRefFactory = that.actorRefFactory
    override val basePath = Path("/")
  }

  def receive = runRoute(restApi.routes ~ healthService.routes)
}
