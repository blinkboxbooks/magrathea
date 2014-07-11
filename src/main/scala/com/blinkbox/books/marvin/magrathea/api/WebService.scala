package com.blinkbox.books.marvin.magrathea.api

import com.blinkbox.books.marvin.magrathea.AppConfig
import spray.routing.HttpServiceActor

class WebService(config: AppConfig) extends HttpServiceActor {
  implicit val executionContext = actorRefFactory.dispatcher
  val restApi = new RestApi(config.service)
  val route = restApi.routes
  def receive = runRoute(route)
}
