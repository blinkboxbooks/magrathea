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

object WebApp extends App with Configuration {
  implicit val system = ActorSystem("akka-spray")
  sys.addShutdownHook(system.shutdown())
  val appConfig = AppConfig(config)
  val service = system.actorOf(Props(classOf[WebService], appConfig))
  val localUrl = appConfig.service.api.localUrl
  IO(Http) ! Http.Bind(service, localUrl.getHost, port = localUrl.effectivePort)
}
