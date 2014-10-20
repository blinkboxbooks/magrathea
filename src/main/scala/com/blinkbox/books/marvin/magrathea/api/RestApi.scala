package com.blinkbox.books.marvin.magrathea.api

import akka.actor.ActorRefFactory
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.ServiceConfig
import com.blinkbox.books.marvin.magrathea.message.DocumentDao
import com.blinkbox.books.spray.{Directives => CommonDirectives, _}
import org.slf4j.LoggerFactory
import spray.http.HttpHeaders.RawHeader
import spray.http.StatusCodes._
import spray.routing._
import spray.util.LoggingContext

import scala.util.control.NonFatal

trait RestRoutes extends HttpService {
  def getLatestBookById: Route
  def search: Route
}

class RestApi(config: ServiceConfig, documentDao: DocumentDao, searchService: SearchService)
  (implicit val actorRefFactory: ActorRefFactory) extends RestRoutes with CommonDirectives with v2.JsonSupport {

  implicit val ec = DiagnosticExecutionContext(actorRefFactory.dispatcher)
  implicit val timeout = config.api.timeout
  implicit val log = LoggerFactory.getLogger(classOf[RestApi])

  override def getLatestBookById = get {
    path("books" / Segment) { id =>
      onSuccess(documentDao.getLatestDocumentById(id))(uncacheable(_))
    }
  }

  override def search = get {
    path("search") {
      parameter('q) { q =>
        paged(defaultCount = 50) { paged =>
          onSuccess(searchService.searchByQuery(q)(paged))(uncacheable(_))
        }
      }
    }
  }

  val routes = rootPath(config.api.localUrl.path) {
    monitor() {
      respondWithHeader(RawHeader("Vary", "Accept, Accept-Encoding")) {
        handleExceptions(exceptionHandler) {
          pathPrefix("magrathea") {
            getLatestBookById ~ search
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
