package com.blinkbox.books.marvin.magrathea

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import com.blinkbox.books.config.Configuration
import com.blinkbox.books.logging.{DiagnosticExecutionContext, Loggers}
import com.blinkbox.books.marvin.magrathea.api.WebService
import com.blinkbox.books.marvin.magrathea.message.MessageListener
import com.blinkbox.books.spray._
import spray.can.Http

object WebApp extends App with Configuration with Loggers {
  val appConfig = AppConfig(config)

  implicit val system = ActorSystem("magrathea-api")
  implicit val ec = DiagnosticExecutionContext(system.dispatcher)
  implicit val timeout = Timeout(appConfig.service.api.timeout)
  sys.addShutdownHook(system.shutdown())
  val service = system.actorOf(Props(classOf[WebService], appConfig), "magrathea-api")
  val localUrl = appConfig.service.api.localUrl
  HttpServer(Http.Bind(service, localUrl.getHost, port = localUrl.effectivePort))

  val messageListener = new MessageListener(appConfig)
  messageListener.start()
}
