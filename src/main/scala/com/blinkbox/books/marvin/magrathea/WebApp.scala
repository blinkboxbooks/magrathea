package com.blinkbox.books.marvin.magrathea

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import com.blinkbox.books.config.Configuration
import com.blinkbox.books.spray._
import spray.can.Http
import spray.routing._

class WebService(config: AppConfig) extends HttpServiceActor {
  implicit val executionContext = actorRefFactory.dispatcher
  val restApi = new RestApi(config.service)
  val route = restApi.routes
  def receive = runRoute(route)
}

class EventListener(config: AppConfig) {
  val eventSystem = ActorSystem("magrathea-event")
  sys.addShutdownHook(eventSystem.shutdown())
}

object WebApp extends App with Configuration {
  val appConfig = AppConfig(config)

  val apiSystem = ActorSystem("magrathea-api")
  sys.addShutdownHook(apiSystem.shutdown())
  val service = apiSystem.actorOf(Props(classOf[WebService], appConfig))
  val localUrl = appConfig.service.api.localUrl
  IO(Http)(apiSystem) ! Http.Bind(service, localUrl.getHost, port = localUrl.effectivePort)

  val eventListener = new EventListener(appConfig)
}
