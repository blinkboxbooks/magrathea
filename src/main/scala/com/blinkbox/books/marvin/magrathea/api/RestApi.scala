package com.blinkbox.books.marvin.magrathea.api

import java.util.UUID

import akka.actor.ActorRefFactory
import com.blinkbox.books.config.ApiConfig
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.message.DocumentDao
import com.blinkbox.books.marvin.magrathea.{JsonDoc, SchemaConfig}
import com.blinkbox.books.spray.v1.Error
import com.blinkbox.books.spray.{Directives => CommonDirectives, _}
import org.slf4j.LoggerFactory
import spray.http.HttpHeaders.RawHeader
import spray.http.StatusCodes._
import spray.routing._
import spray.util.LoggingContext

import scala.util.Try
import scala.util.control.NonFatal

trait RestRoutes extends HttpService {
  def getLatestBookById: Route
  def reIndexBook: Route
  def getLatestContributorById: Route
  def reIndexContributor: Route
  def search: Route
  def reIndexLatestSearch: Route
  def reIndexHistorySearch: Route
}

class RestApi(config: ApiConfig, schemas: SchemaConfig, documentDao: DocumentDao, indexService: IndexService)
  (implicit val actorRefFactory: ActorRefFactory) extends RestRoutes with CommonDirectives with v2.JsonSupport {

  implicit val ec = DiagnosticExecutionContext(actorRefFactory.dispatcher)
  implicit val timeout = config.timeout
  implicit val log = LoggerFactory.getLogger(classOf[RestApi])

  private val bookError = uncacheable(NotFound, Error("NotFound", "The requested book was not found."))
  private val contributorError = uncacheable(NotFound, Error("NotFound", "The requested contributor was not found."))
  private val uuidError = uncacheable(BadRequest, Error("InvalidUUID", "The requested id is not a valid UUID."))

  override val getLatestBookById = get {
    path("books" / Segment) { id =>
      withUUID(id) { uuid =>
        onSuccess(documentDao.getLatestDocumentById(uuid, Option(schemas.book))) {
          _.fold(bookError)(docResponse)
        }
      }
    }
  }

  override val reIndexBook = put {
    path("books" / Segment / "reindex") { id =>
      withUUID(id) { uuid =>
        onSuccess(indexService.reIndexLatestDocument(uuid, schemas.book)) { found =>
          if (found) uncacheable(OK, None) else bookError
        }
      }
    }
  }

  override val getLatestContributorById = get {
    path("contributors" / Segment) { id =>
      withUUID(id) { uuid =>
        onSuccess(documentDao.getLatestDocumentById(uuid, Option(schemas.contributor))) {
          _.fold(contributorError)(docResponse)
        }
      }
    }
  }

  override val reIndexContributor = put {
    path("contributors" / Segment / "reindex") { id =>
      withUUID(id) { uuid =>
        onSuccess(indexService.reIndexLatestDocument(uuid, schemas.contributor)) { found =>
          if (found) uncacheable(OK, None) else contributorError
        }
      }
    }
  }

  override val search = get {
    path("search") {
      parameter('q) { q =>
        paged(defaultCount = 50) { paged =>
          onSuccess(indexService.searchInLatest(q, paged))(uncacheable(_))
        }
      }
    }
  }

  override val reIndexLatestSearch = put {
    path("search" / "reindex" / "latest") {
      dynamic {
        log.info("Starting re-indexing of 'latest'...")
        indexService.reIndexLatest().onComplete {
          case scala.util.Success(_) => log.info("Re-indexing of 'latest' finished successfully.")
          case scala.util.Failure(e) => log.error("Re-indexing of 'latest' failed.", e)
        }
        uncacheable(Accepted, None)
      }
    }
  }

  override val reIndexHistorySearch = put {
    path("search" / "reindex" / "history") {
      dynamic {
        log.info("Starting re-indexing of 'history'...")
        indexService.reIndexHistory().onComplete {
          case scala.util.Success(_) => log.info("Re-indexing of 'history' finished successfully.")
          case scala.util.Failure(e) => log.error("Re-indexing of 'history' failed.", e)
        }
        uncacheable(Accepted, None)
      }
    }
  }

  val routes = rootPath(config.localUrl.path) {
    monitor() {
      respondWithHeader(RawHeader("Vary", "Accept, Accept-Encoding")) {
        handleExceptions(exceptionHandler) {
          getLatestBookById ~ getLatestContributorById ~ search ~
          reIndexBook ~ reIndexContributor ~ reIndexLatestSearch ~ reIndexHistorySearch
        }
      }
    }
  }

  private def exceptionHandler(implicit log: LoggingContext) = ExceptionHandler {
    case NonFatal(e) =>
      log.error(e, "Unhandled error")
      uncacheable(InternalServerError, None)
  }

  private def withUUID(rawId: String): Directive1[UUID] =
    Try(UUID.fromString(rawId)).toOption.fold[Directive1[UUID]](uuidError)(provide)

  private def docResponse = (doc: JsonDoc) => uncacheable(doc.toJson)
}
