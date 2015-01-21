package com.blinkbox.books.marvin.magrathea.message

import com.blinkbox.books.elasticsearch.client._
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.api.DefaultIndexService
import com.blinkbox.books.marvin.magrathea.{ElasticConfig, TestHelper}
import com.blinkbox.books.spray.Page
import com.blinkbox.books.spray.v2.ListPage
import com.blinkbox.books.test.MockitoSyrup
import org.elasticsearch.action.search.SearchRequest
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, Matchers}
import spray.http.StatusCodes
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.Future
import scala.language.{implicitConversions, postfixOps}

@RunWith(classOf[JUnitRunner])
class IndexServiceTest extends FlatSpecLike with Matchers with MockitoSyrup with ScalaFutures
  with Json4sJacksonSupport with TestHelper {
  implicit val json4sJacksonFormats = DefaultFormats ++ Formats.all

  val config = mock[ElasticConfig]
  doReturn("magrathea").when(config).index
  doReturn(5).when(config).reIndexChunks
  val elasticClient = mock[ElasticClient]
  val documentDao = mock[DocumentDao]

  val indexService = new DefaultIndexService(elasticClient, config, documentDao)

  behavior of "The index service"

  it should "return an empty list while searching in current documents, if the index is not created" in {
    doReturn(Future.failed(UnsuccessfulResponse(StatusCodes.NotFound, "")))
      .when(elasticClient).execute(any[SearchRequest])(anyObject(), anyObject())
    whenReady(indexService.searchInCurrent("*", Page(0, 50))) { result =>
      result shouldEqual ListPage(List.empty, lastPage = true)
    }
  }

  it should "return an empty list while searching in history documents, if the index is not created" in {
    doReturn(Future.failed(UnsuccessfulResponse(StatusCodes.NotFound, "")))
      .when(elasticClient).execute(any[SearchRequest])(anyObject(), anyObject())
    whenReady(indexService.searchInHistory("*", Page(0, 50))) { result =>
      result shouldEqual ListPage(List.empty, lastPage = true)
    }
  }
}
