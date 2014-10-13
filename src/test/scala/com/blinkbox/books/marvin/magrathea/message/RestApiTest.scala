package com.blinkbox.books.marvin.magrathea.message

import java.net.URL

import com.blinkbox.books.config.ApiConfig
import com.blinkbox.books.marvin.magrathea.ServiceConfig
import com.blinkbox.books.marvin.magrathea.api.RestApi
import com.blinkbox.books.spray.v2
import com.blinkbox.books.test.MockitoSyrup
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
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
  with JsonMethods with Matchers {
  implicit val actorRefFactory = system
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  val config = mock[ServiceConfig]
  val apiConfig = mock[ApiConfig]
  when(apiConfig.localUrl).thenReturn(new URL("http://localhost"))
  when(apiConfig.externalUrl).thenReturn(new URL("http://localhost"))
  when(apiConfig.timeout).thenReturn(5.seconds)
  when(config.api).thenReturn(apiConfig)

  val documentDao = mock[DocumentDao]
  val routes = new RestApi(config, documentDao).routes

  "The service" should "return 200 with the requested book, if it exists" in {
    val book: JValue = "field" -> "value"
    doReturn(Future.successful(Some(book))).when(documentDao).getLatestDocumentById(any[String])
    Get("/magrathea/books/abc") ~> routes ~> check {
      status shouldEqual OK
      body.asString shouldEqual compact(render(book))
    }
  }

  it should "return 404 if the book does not exist" in {
    doReturn(Future.successful(None)).when(documentDao).getLatestDocumentById(any[String])
    Get("/magrathea/books/oops") ~> routes ~> check {
      status shouldEqual NotFound
    }
  }
}
