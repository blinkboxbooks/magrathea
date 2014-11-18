package com.blinkbox.books.marvin.magrathea

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import com.blinkbox.books.config.Configuration
import com.blinkbox.books.logging.{DiagnosticExecutionContext, Loggers}
import com.blinkbox.books.marvin.magrathea.api.{DefaultIndexService, WebService}
import com.blinkbox.books.marvin.magrathea.message.{MessageListener, PostgresDocumentDao}
import com.blinkbox.books.spray._
import com.sksamuel.elastic4s.ElasticClient
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.elasticsearch.common.settings.ImmutableSettings
import spray.can.Http

import scala.util.control.ControlThrowable

object WebApp extends App with Configuration with Loggers with StrictLogging {
  try {
    val appConfig = AppConfig(config)

    val elasticSettings = ImmutableSettings.settingsBuilder().put("cluster.name", appConfig.elasticsearch.cluster).build()
    val elasticClient = ElasticClient.remote(elasticSettings, (appConfig.elasticsearch.host, appConfig.elasticsearch.port))

    val apiSystem = ActorSystem("magrathea-api-system", config)
    val apiExCtx = DiagnosticExecutionContext(apiSystem.dispatcher)
    val apiTimeout = Timeout(appConfig.api.timeout)
    sys.addShutdownHook(apiSystem.shutdown())
    val apiDocumentDao = new PostgresDocumentDao(appConfig.db, appConfig.schemas)
    val apiIndexService = new DefaultIndexService(elasticClient, appConfig.elasticsearch, apiDocumentDao)
    val service = apiSystem.actorOf(Props(classOf[WebService], appConfig, apiDocumentDao, apiIndexService), "magrathea-api")
    val localUrl = appConfig.api.localUrl
    HttpServer(Http.Bind(service, localUrl.getHost, port = localUrl.effectivePort))(apiSystem, apiExCtx, apiTimeout)

    val msgSystem = ActorSystem("magrathea-msg-system", config)
    val msgExCtx = DiagnosticExecutionContext(msgSystem.dispatcher)
    val msgTimeout = Timeout(appConfig.listener.actorTimeout)
    sys.addShutdownHook(msgSystem.shutdown())
    val msgDocumentDao = new PostgresDocumentDao(appConfig.db, appConfig.schemas)
    val msgIndexService = new DefaultIndexService(elasticClient, appConfig.elasticsearch, msgDocumentDao)
    val messageListener = new MessageListener(appConfig, msgIndexService, msgDocumentDao)(msgSystem, msgExCtx, msgTimeout)
    messageListener.start()
  } catch {
    case ex: ControlThrowable => throw ex
    case ex: Throwable =>
      logger.error("Error during initialisation of the service", ex)
      System.exit(1)
  }
}
