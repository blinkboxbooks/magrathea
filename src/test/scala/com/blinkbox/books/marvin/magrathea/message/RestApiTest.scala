package com.blinkbox.books.marvin.magrathea.message

import java.net.URL
import java.util.UUID

import com.blinkbox.books.config.ApiConfig
import com.blinkbox.books.marvin.magrathea.SchemaConfig
import com.blinkbox.books.marvin.magrathea.api.{IndexService, RestApi}
import com.blinkbox.books.spray.v2
import com.blinkbox.books.test.MockitoSyrup
import org.json4s.jackson.JsonMethods
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, Matchers}
import spray.http.StatusCodes._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest

import scala.concurrent.Future
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class RestApiTest extends FlatSpecLike with ScalatestRouteTest with HttpService with MockitoSyrup with v2.JsonSupport
  with JsonMethods with Matchers with TestHelper {
  implicit val actorRefFactory = system
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  val config = mock[ApiConfig]
  when(config.localUrl).thenReturn(new URL("http://localhost"))
  when(config.externalUrl).thenReturn(new URL("http://localhost"))
  when(config.timeout).thenReturn(5.seconds)
  val schemas = mock[SchemaConfig]
  when(schemas.book).thenReturn("ingestion.book.metadata.v2")
  when(schemas.contributor).thenReturn("ingestion.contributor.metadata.v2")
  val indexService = mock[IndexService]

  val documentDao = mock[DocumentDao]
  val routes = new RestApi(config, schemas, documentDao, indexService).routes

  "The service" should "return 200 with the requested book, if it exists" in {
    val book = sampleBook()
    when(documentDao.getLatestDocumentById(any[UUID], any[Option[String]])).thenReturn(
      Future.successful(Some(latest(book))))
    Get(s"/books/${UUID.randomUUID()}") ~> routes ~> check {
      status shouldEqual OK
      body.asString shouldEqual compact(render(book))
    }
  }

  it should "return 400 if the book id is not a UUID" in {
    Get("/books/xxx") ~> routes ~> check {
      status shouldEqual BadRequest
    }
  }

  it should "return 404 if the book does not exist" in {
    when(documentDao.getLatestDocumentById(any[UUID], any[Option[String]])).thenReturn(
      Future.successful(None))
    Get(s"/books/${UUID.randomUUID()}") ~> routes ~> check {
      status shouldEqual NotFound
    }
  }

  it should "return 200 and re-index a book, if it exists" in {
    when(indexService.reIndexLatestDocument(any[UUID], any[String])).thenReturn(Future.successful(true))
    Put(s"/books/${UUID.randomUUID()}/reindex") ~> routes ~> check {
      status shouldEqual OK
    }
  }

  it should "return 400 if the requested book's id to re-index is not a UUID" in {
    Put("/books/xxx/reindex") ~> routes ~> check {
      status shouldEqual BadRequest
    }
  }

  it should "return 404 if the requested book to re-index does not exist" in {
    when(indexService.reIndexLatestDocument(any[UUID], any[String])).thenReturn(Future.successful(false))
    Put(s"/books/${UUID.randomUUID()}/reindex") ~> routes ~> check {
      status shouldEqual NotFound
    }
  }

  it should "return 200 with the requested contributor, if it exists" in {
    val contributor = sampleContributor()
    when(documentDao.getLatestDocumentById(any[UUID], any[Option[String]])).thenReturn(
      Future.successful(Some(latest(contributor))))
    Get(s"/contributors/${UUID.randomUUID()}") ~> routes ~> check {
      status shouldEqual OK
      body.asString shouldEqual compact(render(contributor))
    }
  }

  it should "return 400 if the contributor id is not a UUID" in {
    Get("/contributors/xxx") ~> routes ~> check {
      status shouldEqual BadRequest
    }
  }

  it should "return 404 if the contributor does not exist" in {
    when(documentDao.getLatestDocumentById(any[UUID], any[Option[String]])).thenReturn(
      Future.successful(None))
    Get(s"/contributors/${UUID.randomUUID()}") ~> routes ~> check {
      status shouldEqual NotFound
    }
  }

  it should "return 200 and re-index a contributor, if it exists" in {
    when(indexService.reIndexLatestDocument(any[UUID], any[String])).thenReturn(Future.successful(true))
    Put(s"/contributors/${UUID.randomUUID()}/reindex") ~> routes ~> check {
      status shouldEqual OK
    }
  }

  it should "return 400 if the requested contributor's id to re-index is not a UUID" in {
    Put("/contributors/xxx/reindex") ~> routes ~> check {
      status shouldEqual BadRequest
    }
  }

  it should "return 404 if the requested contributor to re-index does not exist" in {
    when(indexService.reIndexLatestDocument(any[UUID], any[String])).thenReturn(Future.successful(false))
    Put(s"/contributors/${UUID.randomUUID()}/reindex") ~> routes ~> check {
      status shouldEqual NotFound
    }
  }
}
