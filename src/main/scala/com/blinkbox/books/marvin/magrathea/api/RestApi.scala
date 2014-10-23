package com.blinkbox.books.marvin.magrathea.api

import akka.actor.ActorRefFactory
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.message.DocumentDao
import com.blinkbox.books.marvin.magrathea.{SchemaConfig, ServiceConfig}
import com.blinkbox.books.spray.v1.Error
import com.blinkbox.books.spray.{Directives => CommonDirectives, _}
import org.json4s.JsonAST.JString
import org.slf4j.LoggerFactory
import spray.http.HttpHeaders.RawHeader
import spray.http.StatusCodes._
import spray.routing._
import spray.util.LoggingContext

import scala.util.control.NonFatal

trait RestRoutes extends HttpService {
  def getLatestBookById: Route
  def reIndexBook: Route
  def getLatestContributorById: Route
  def reIndexContributor: Route
  def search: Route
  def reIndexSearch: Route
}

class RestApi(config: ServiceConfig, schemas: SchemaConfig, documentDao: DocumentDao, searchService: SearchService)
  (implicit val actorRefFactory: ActorRefFactory) extends RestRoutes with CommonDirectives with v2.JsonSupport {

  implicit val ec = DiagnosticExecutionContext(actorRefFactory.dispatcher)
  implicit val timeout = config.api.timeout
  implicit val log = LoggerFactory.getLogger(classOf[RestApi])

  override val getLatestBookById = get {
    path("books" / Segment) { id =>
      onSuccess(documentDao.getLatestDocumentById(id)) {
        case Some(doc) if doc \ "$schema" == JString(schemas.book) => uncacheable(doc)
        case _ => uncacheable(NotFound, Error("not_found", "The requested book was not found."))
      }
    }
  }

  override val reIndexBook = put {
    path("books" / Segment / "reindex") { id =>
      uncacheable(OK, None)
    }
  }

  override val getLatestContributorById = get {
    path("contributors" / Segment) { id =>
      onSuccess(documentDao.getLatestDocumentById(id)) {
        case Some(doc) if doc \ "$schema" == JString(schemas.contributor) => uncacheable(doc)
        case _ => uncacheable(NotFound, Error("not_found", "The requested contributor was not found."))
      }
    }
  }

  override val reIndexContributor = put {
    path("contributor" / Segment / "reindex") { id =>
      uncacheable(OK, None)
    }
  }

  override val search = get {
    path("search") {
      parameter('q) { q =>
        paged(defaultCount = 50) { paged =>
          onSuccess(searchService.searchByQuery(q)(paged))(uncacheable(_))
        }
      }
    }
  }

  override val reIndexSearch = put {
    path("search/reindex") {
      uncacheable(OK, None)
    }
  }

  val routes = rootPath(config.api.localUrl.path) {
    monitor() {
      respondWithHeader(RawHeader("Vary", "Accept, Accept-Encoding")) {
        handleExceptions(exceptionHandler) {
          getLatestBookById ~ getLatestContributorById ~ search ~
          reIndexBook ~ reIndexContributor ~ reIndexSearch
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
