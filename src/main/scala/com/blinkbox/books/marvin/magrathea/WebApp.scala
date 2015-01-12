package com.blinkbox.books.marvin.magrathea

import java.util.concurrent.ForkJoinPool

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import com.blinkbox.books.config.Configuration
import com.blinkbox.books.elasticsearch.client.SprayElasticClient
import com.blinkbox.books.logging.{DiagnosticExecutionContext, Loggers}
import com.blinkbox.books.marvin.magrathea.api.{DefaultIndexService, WebService}
import com.blinkbox.books.marvin.magrathea.message.{MessageListener, PostgresDocumentDao}
import com.blinkbox.books.spray._
import com.typesafe.scalalogging.StrictLogging
import spray.can.Http

import scala.concurrent.ExecutionContext

object WebApp extends App with Configuration with Loggers with StrictLogging {
  try {
    val appConfig = AppConfig(config)

    val system = ActorSystem("magrathea-system", config)
    val elasticEc = DiagnosticExecutionContext(ExecutionContext.fromExecutor(new ForkJoinPool()))
    val elasticClient = new SprayElasticClient(appConfig.elasticsearch.host, appConfig.elasticsearch.port)(system, elasticEc)

    val apiExCtx = DiagnosticExecutionContext(system.dispatcher)
    val apiTimeout = Timeout(appConfig.api.timeout)
    sys.addShutdownHook(system.shutdown())
    val apiDocumentDao = new PostgresDocumentDao(appConfig.db, appConfig.schemas)
    val apiIndexService = new DefaultIndexService(elasticClient, appConfig.elasticsearch, apiDocumentDao)
    val service = system.actorOf(Props(classOf[WebService], appConfig, apiDocumentDao, apiIndexService), "magrathea-api")
    val localUrl = appConfig.api.localUrl
    HttpServer(Http.Bind(service, localUrl.getHost, port = localUrl.effectivePort))(system, apiExCtx, apiTimeout)

    val msgExCtx = DiagnosticExecutionContext(ExecutionContext.fromExecutor(new ForkJoinPool()))
    val msgTimeout = Timeout(appConfig.listener.actorTimeout)
    sys.addShutdownHook(system.shutdown())
    val msgDocumentDao = new PostgresDocumentDao(appConfig.db, appConfig.schemas)
    val msgIndexService = new DefaultIndexService(elasticClient, appConfig.elasticsearch, msgDocumentDao)
    val messageListener = new MessageListener(appConfig, msgIndexService, msgDocumentDao)(system, msgExCtx, msgTimeout)
    messageListener.start()
  } catch {
    case ex: Throwable =>
      logger.error("Error during initialisation of the service", ex)
      System.exit(1)
  }
}
