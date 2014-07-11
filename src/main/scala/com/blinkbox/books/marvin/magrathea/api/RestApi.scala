package com.blinkbox.books.marvin.magrathea.api

import akka.actor.ActorRefFactory
import com.blinkbox.books.marvin.magrathea.ServiceConfig
import com.blinkbox.books.spray.JsonFormats.ExplicitTypeHints
import com.blinkbox.books.spray.v1.{ListPage, Version1JsonSupport}
import com.blinkbox.books.spray.{Directives => CommonDirectives, _}
import shapeless.HNil
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

  implicit val executionContext = actorRefFactory.dispatcher
  implicit val timeout = config.api.timeout
  override val responseTypeHints = ExplicitTypeHints(Map(
    classOf[ListPage[_]] -> "urn:blinkboxbooks:schema:list"))

  override def getAll: Route = {
    get {
      pathEndOrSingleSlash {
        complete(OK, config.myKey)
      }
    }
  }

  val routes = rawPathPrefix(PathMatcher[HNil](config.api.externalUrl.path, HNil)) {
    respondWithHeader(RawHeader("Vary", "Accept, Accept-Encoding")) {
      handleExceptions(exceptionHandler) {
        getAll
      }
    }
  }

  private def exceptionHandler(implicit log: LoggingContext) = ExceptionHandler {
    case NonFatal(e) =>
      log.error(e, "Unhandled error")
      uncacheable(InternalServerError, None)
  }
}
