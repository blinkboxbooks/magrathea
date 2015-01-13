package com.blinkbox.books.marvin.magrathea.api

import java.net.URL
import java.util.UUID

import com.blinkbox.books.config.ApiConfig
import com.blinkbox.books.marvin.magrathea.message.{DocumentDao, Revision}
import com.blinkbox.books.marvin.magrathea.{History, SchemaConfig, TestHelper}
import com.blinkbox.books.spray.v2.Error
import com.blinkbox.books.test.MockitoSyrup
import org.json4s.JsonAST.{JNothing, JValue}
import org.json4s.JsonDSL._
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, Matchers}
import spray.http.AllOrigins
import spray.http.HttpHeaders.`Access-Control-Allow-Origin`
import spray.http.StatusCodes._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest

import scala.concurrent.Future
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class RestApiTest extends FlatSpecLike with ScalatestRouteTest with HttpService
  with MockitoSyrup with Matchers with TestHelper {
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
    when(documentDao.getCurrentDocumentById(any[UUID], any[Option[String]])).thenReturn(
      Future.successful(Some(current(book))))
    Get(s"/books/$generateId") ~> routes ~> check {
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
    when(documentDao.getCurrentDocumentById(any[UUID], any[Option[String]])).thenReturn(
      Future.successful(None))
    Get(s"/books/$generateId") ~> routes ~> check {
      status shouldEqual NotFound
    }
  }

  it should "return 200 and list the book's revision list" in {
    when(documentDao.getDocumentHistory(any[UUID], any[String])).thenReturn(Future.successful(List(
      history(sampleBook("fieldA" -> "valueA")),
      history(sampleBook("fieldB" -> "valueB")),
      history(sampleBook("fieldA" -> "test"))
    )))
    val changed0: JValue = "fieldA" -> "test"
    val added1: JValue = "fieldB" -> "valueB"
    val added2: JValue = "fieldA" -> "valueA"
    Get(s"/books/$generateId/history") ~> routes ~> check {
      status shouldEqual OK
      val resp = responseAs[List[Revision]]
      resp.size shouldEqual 3
      List(resp(0).added, resp(0).changed, resp(0).deleted) shouldEqual List(JNothing, changed0, JNothing)
      List(resp(1).added, resp(1).changed, resp(1).deleted) shouldEqual List(added1, JNothing, JNothing)
      List(resp(2).added, resp(2).changed, resp(2).deleted) shouldEqual List(added2, JNothing, JNothing)
    }
  }

  it should "return 400 getting the history of a book with an invalid id" in {
    Get("/books/xxx/history") ~> routes ~> check {
      status shouldEqual BadRequest
    }
  }

  it should "return 404 getting the history of a book that does not exist" in {
    when(documentDao.getDocumentHistory(any[UUID], any[String])).thenReturn(Future.successful(List.empty[History]))
    Get(s"/books/$generateId/history") ~> routes ~> check {
      status shouldEqual NotFound
    }
  }

  it should "return 200 and list the contributor's revision list" in {
    when(documentDao.getDocumentHistory(any[UUID], any[String])).thenReturn(Future.successful(List(
      history(sampleContributor("fieldA" -> "valueA")),
      history(sampleContributor("fieldB" -> "valueB")),
      history(sampleContributor("fieldA" -> "test"))
    )))
    val changed0: JValue = "fieldA" -> "test"
    val added1: JValue = "fieldB" -> "valueB"
    val added2: JValue = "fieldA" -> "valueA"
    Get(s"/contributors/$generateId/history") ~> routes ~> check {
      status shouldEqual OK
      val resp = responseAs[List[Revision]]
      resp.size shouldEqual 3
      List(resp(0).added, resp(0).changed, resp(0).deleted) shouldEqual List(JNothing, changed0, JNothing)
      List(resp(1).added, resp(1).changed, resp(1).deleted) shouldEqual List(added1, JNothing, JNothing)
      List(resp(2).added, resp(2).changed, resp(2).deleted) shouldEqual List(added2, JNothing, JNothing)
    }
  }

  it should "return 400 getting the history of a contributor with an invalid id" in {
    Get("/contributors/xxx/history") ~> routes ~> check {
      status shouldEqual BadRequest
    }
  }

  it should "return 404 getting the history of a contributor that does not exist" in {
    when(documentDao.getDocumentHistory(any[UUID], any[String])).thenReturn(Future.successful(List.empty[History]))
    Get(s"/contributors/$generateId/history") ~> routes ~> check {
      status shouldEqual NotFound
    }
  }

  it should "return 200 and re-index a book, if it exists" in {
    when(indexService.reIndexCurrentDocument(any[UUID], any[String])).thenReturn(Future.successful(true))
    Put(s"/books/$generateId/reindex") ~> routes ~> check {
      status shouldEqual OK
    }
  }

  it should "return 400 if the requested book's id to re-index is not a UUID" in {
    Put("/books/xxx/reindex") ~> routes ~> check {
      status shouldEqual BadRequest
    }
  }

  it should "return 404 if the requested book to re-index does not exist" in {
    when(indexService.reIndexCurrentDocument(any[UUID], any[String])).thenReturn(Future.successful(false))
    Put(s"/books/$generateId/reindex") ~> routes ~> check {
      status shouldEqual NotFound
    }
  }

  it should "return 200 with the requested contributor, if it exists" in {
    val contributor = sampleContributor()
    when(documentDao.getCurrentDocumentById(any[UUID], any[Option[String]])).thenReturn(
      Future.successful(Some(current(contributor))))
    Get(s"/contributors/$generateId") ~> routes ~> check {
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
    when(documentDao.getCurrentDocumentById(any[UUID], any[Option[String]])).thenReturn(
      Future.successful(None))
    Get(s"/contributors/$generateId") ~> routes ~> check {
      status shouldEqual NotFound
    }
  }

  it should "return 200 and re-index a contributor, if it exists" in {
    when(indexService.reIndexCurrentDocument(any[UUID], any[String])).thenReturn(Future.successful(true))
    Put(s"/contributors/$generateId/reindex") ~> routes ~> check {
      status shouldEqual OK
    }
  }

  it should "return 400 if the requested contributor's id to re-index is not a UUID" in {
    Put("/contributors/xxx/reindex") ~> routes ~> check {
      status shouldEqual BadRequest
    }
  }

  it should "return 404 if the requested contributor to re-index does not exist" in {
    when(indexService.reIndexCurrentDocument(any[UUID], any[String])).thenReturn(Future.successful(false))
    Put(s"/contributors/$generateId/reindex") ~> routes ~> check {
      status shouldEqual NotFound
    }
  }

  it should "include CORS headers" in {
    Get("/books/xxx") ~> routes ~> check {
      header("Access-Control-Allow-Origin") shouldEqual Some(`Access-Control-Allow-Origin`(AllOrigins))
    }
  }

  it should "throw a json exception for a bad request" in {
    Get("/search?q=foo&count=string") ~> routes ~> check {
      status shouldEqual BadRequest
      responseAs[Error].code shouldEqual "BadRequest"
    }
  }
}
