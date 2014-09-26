package com.blinkbox.books.marvin.magrathea.api

import akka.actor.ActorRefFactory
import com.blinkbox.books.json.ExplicitTypeHints
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.ServiceConfig
import com.blinkbox.books.spray.v1.{ListPage, Version1JsonSupport}
import com.blinkbox.books.spray.{Directives => CommonDirectives, _}
import org.slf4j.LoggerFactory
import spray.http.HttpHeaders.RawHeader
import spray.http.StatusCodes._
import spray.routing._
import spray.util.LoggingContext

import scala.util.control.NonFatal

trait RestRoutes extends HttpService {
  def getAll: Route
}

class RestApi(config: ServiceConfig)(implicit val actorRefFactory: ActorRefFactory)
  extends RestRoutes with CommonDirectives with Version1JsonSupport {

  implicit val executionContext = DiagnosticExecutionContext(actorRefFactory.dispatcher)
  implicit val timeout = config.api.timeout
  implicit val log = LoggerFactory.getLogger(classOf[RestApi])
  override val responseTypeHints = ExplicitTypeHints(Map(
    classOf[ListPage[_]] -> "urn:blinkboxbooks:schema:list"))

  override def getAll: Route = get {
    pathEndOrSingleSlash {
      uncacheable(s"myKey = ${config.myKey.toString}")
    }
  }

  val routes = rootPath(config.api.localUrl.path) {
    monitor() {
      respondWithHeader(RawHeader("Vary", "Accept, Accept-Encoding")) {
        handleExceptions(exceptionHandler) {
          path("magrathea") {
            getAll
          }
        }
      }
    }
  }

  private def exceptionHandler(implicit log: LoggingContext) = ExceptionHandler {
    case NonFatal(e) =>
      log.error(e, "Unhandled error")
      uncacheable(InternalServerError, None)
  }
}
