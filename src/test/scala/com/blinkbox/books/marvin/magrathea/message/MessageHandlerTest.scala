package com.blinkbox.books.marvin.magrathea.message

import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, Props, Status}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.marvin.magrathea.api.IndexService
import com.blinkbox.books.marvin.magrathea.{SchemaConfig, TestHelper}
import com.blinkbox.books.messaging._
import com.blinkbox.books.test.MockitoSyrup
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexResponse
import org.json4s.JsonAST.{JArray, JNothing, JString, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import spray.can.Http.ConnectionException
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.duration._
import scala.concurrent.{Future, TimeoutException}
import scala.language.implicitConversions

@RunWith(classOf[JUnitRunner])
class MessageHandlerTest extends TestKit(ActorSystem("test-system")) with ImplicitSender with FlatSpecLike
  with BeforeAndAfterAll with MockitoSyrup with Matchers with Json4sJacksonSupport with JsonMethods {

  implicit val json4sJacksonFormats = DefaultFormats
  val retryInterval = 100.millis

  override protected def afterAll(): Unit = system.shutdown()

  private def testMerge(docA: JValue, docB: JValue): JValue = docA merge docB

  "A message handler" should "handle a book message" in new TestFixture {
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
  }

  it should "classify contributors and add bbb_id" in new TestFixture {
    val contributors: JValue = "contributors" -> List(
      ("names" -> (("display" -> "Zaphod Beeblebrox") ~ ("sort" -> "Beeblebrox, Zaphod"))) ~
      ("role" -> "Author") ~ ("biography" -> "Zaphod is just some guy, you know?")
      ,
      ("names" -> (("display" -> "Anargyros Akrivos") ~ ("sort" -> "Akrivos, Anargyros"))) ~
      ("role" -> "Author") ~ ("biography" -> "Argy is just some guy, you know?")
    )
    handler ! bookEvent(sampleBook(contributors))
    checkNoFailures()
    expectMsgType[Status.Success]
    val captor = ArgumentCaptor.forClass(classOf[JValue])
    verify(documentDao, times(1)).storeHistoryDocument(captor.capture(), eql(true))
    val nameHash0 = ((contributors \ "contributors")(0) \ "names" \ "display").sha1
    val nameHash1 = ((contributors \ "contributors")(1) \ "names" \ "display").sha1
    val cl0 = JArray(List[JValue](("realm" -> "bbb_id") ~ ("id" -> nameHash0)))
    val cl1 = JArray(List[JValue](("realm" -> "bbb_id") ~ ("id" -> nameHash1)))
    val ids0: JValue = "bbb" -> nameHash0
    val ids1: JValue = "bbb" -> nameHash1
    val cArray = captor.getValue \ "contributors"
    cArray.children.size shouldEqual 2
    cArray(0) \ "classification" shouldEqual cl0
    cArray(0) \ "ids" shouldEqual ids0
    cArray(1) \ "classification" shouldEqual cl1
    cArray(1) \ "ids" shouldEqual ids1
  }

  it should "keep contributor classification if exists" in new TestFixture {
    val contributors: JValue = "contributors" -> List(
      ("names" -> (("display" -> "Zaphod Beeblebrox") ~ ("sort" -> "Beeblebrox, Zaphod"))) ~
      ("role" -> "Author") ~ ("biography" -> "Zaphod is just some guy, you know?") ~
      ("classification" -> List("AA"))
      ,
      ("names" -> (("display" -> "Anargyros Akrivos") ~ ("sort" -> "Akrivos, Anargyros"))) ~
      ("role" -> "Author") ~ ("biography" -> "Argy is just some guy, you know?")
    )
    handler ! bookEvent(sampleBook(contributors))
    checkNoFailures()
    expectMsgType[Status.Success]
    val captor = ArgumentCaptor.forClass(classOf[JValue])
    verify(documentDao, times(1)).storeHistoryDocument(captor.capture(), eql(true))
    val nameHash1 = ((contributors \ "contributors")(1) \ "names" \ "display").sha1
    val cl1 = JArray(List[JValue](("realm" -> "bbb_id") ~ ("id" -> nameHash1)))
    val cArray = captor.getValue \ "contributors"
    cArray.children.size shouldEqual 2
    cArray(0) \ "classification" shouldEqual JArray(List("AA"))
    cArray(1) \ "classification" shouldEqual cl1
  }

  it should "merge with existing contributor ids" in new TestFixture {
    val contributors: JValue = "contributors" -> List(
      ("names" -> (("display" -> "Zaphod Beeblebrox") ~ ("sort" -> "Beeblebrox, Zaphod"))) ~
      ("role" -> "Author") ~ ("biography" -> "Zaphod is just some guy, you know?") ~
      ("ids" -> (("xxx" -> "AA") ~ ("bbb" -> "BB")))
      ,
      ("names" -> (("display" -> "Anargyros Akrivos") ~ ("sort" -> "Akrivos, Anargyros"))) ~
      ("role" -> "Author") ~ ("biography" -> "Argy is just some guy, you know?")
    )
    handler ! bookEvent(sampleBook(contributors))
    checkNoFailures()
    expectMsgType[Status.Success]
    val captor = ArgumentCaptor.forClass(classOf[JValue])
    verify(documentDao, times(1)).storeHistoryDocument(captor.capture(), eql(true))
    val nameHash1 = ((contributors \ "contributors")(1) \ "names" \ "display").sha1
    val ids0: JValue = ("xxx" -> "AA") ~ ("bbb" -> "BB")
    val ids1: JValue = "bbb" -> nameHash1
    val cArray = captor.getValue \ "contributors"
    cArray.children.size shouldEqual 2
    cArray(0) \ "ids" shouldEqual ids0
    cArray(1) \ "ids" shouldEqual ids1
  }

  it should "index every current document that it stores" in new TestFixture {
    val inserted = UUID.randomUUID()
    val deleted = List.empty
    val captor = ArgumentCaptor.forClass(classOf[JValue])
    doReturn(Future.successful((inserted, deleted))).when(documentDao).storeCurrentDocument(captor.capture(), any[Boolean])
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
    verify(indexService, times(1)).indexCurrentDocument(inserted, captor.getValue)
  }

  it should "delete current index from the deleted current documents" in new TestFixture {
    val inserted = UUID.randomUUID()
    val deleted = List(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
    doReturn(Future.successful((inserted, deleted))).when(documentDao).storeCurrentDocument(any[JValue], any[Boolean])
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
    verify(indexService, times(1)).deleteCurrentIndex(deleted)
  }

  it should "merge even if the history has only one document" in new TestFixture {
    doReturn(Future.successful(List(history(sampleBook())))).when(documentDao).getDocumentHistory(any[JValue])
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
  }

  it should "not try to merge an empty history list" in new TestFixture {
    val event = bookEvent(sampleBook())
    doReturn(Future.successful(List.empty)).when(documentDao).getDocumentHistory(any[JValue])
    handler ! event
    checkFailure[IllegalArgumentException](event)
    expectMsgType[Status.Success]
  }

  it should "recover from a temporary connection failure" in new TestFixture {
    when(documentDao.storeHistoryDocument(any[JValue], any[Boolean]))
      .thenReturn(Future.failed(new TimeoutException()))
      .thenReturn(Future.failed(new ConnectionException("oops")))
      .thenReturn(Future.successful(UUID.randomUUID(), List.empty))
    handler ! bookEvent(sampleBook())
    expectMsgType[Status.Success]
    checkNoFailures()
  }

  it should "trigger additional contributor merges if contributors are included in a document" in new TestFixture {
    val contributors: JValue = sampleBook("contributors" -> List(
      ("names" -> (("display" -> "Zaphod Beeblebrox") ~ ("sort" -> "Beeblebrox, Zaphod"))) ~
      ("role" -> "Author") ~ ("biography" -> "Zaphod is just some guy, you know?")
      ,
      ("names" -> (("display" -> "Anargyros Akrivos") ~ ("sort" -> "Akrivos, Anargyros"))) ~
      ("role" -> "Author") ~ ("biography" -> "Argy is just some guy, you know?")
    ))
    handler ! bookEvent(contributors)
    checkNoFailures()
    expectMsgType[Status.Success]
    val historyCaptor = ArgumentCaptor.forClass(classOf[JValue])
    verify(documentDao, times(1)).storeHistoryDocument(any[JValue], eql(true))
    verify(documentDao, times(2)).storeHistoryDocument(historyCaptor.capture(), eql(false))
    historyCaptor.getAllValues.size() shouldEqual 2
    historyCaptor.getAllValues.get(0) \ "$schema" shouldEqual JString("ingestion.contributor.metadata.v2")
    historyCaptor.getAllValues.get(0) \ "source" shouldEqual contributors \ "source"
    historyCaptor.getAllValues.get(1) \ "$schema" shouldEqual JString("ingestion.contributor.metadata.v2")
    historyCaptor.getAllValues.get(1) \ "source" shouldEqual contributors \ "source"
    verify(documentDao, times(1)).storeCurrentDocument(any[JValue], eql(true))
    verify(documentDao, times(2)).storeCurrentDocument(any[JValue], eql(false))
  }

  trait TestFixture extends TestHelper {
    val config = mock[SchemaConfig]
    doReturn("ingestion.book.metadata.v2").when(config).book
    doReturn("ingestion.contributor.metadata.v2").when(config).contributor

    val documentDao = mock[DocumentDao]

    private val deletedDocuments = List.empty
    doReturn(Future.successful(UUID.randomUUID(), deletedDocuments)).when(documentDao).storeHistoryDocument(any[JValue], any[Boolean])
    doReturn(Future.successful(UUID.randomUUID(), deletedDocuments)).when(documentDao).storeCurrentDocument(any[JValue], any[Boolean])

    private val documentHistory = List(history(sampleBook()), history(sampleBook()), history(sampleBook()))
    doReturn(Future.successful(documentHistory)).when(documentDao).getDocumentHistory(any[JValue])

    val distributor = mock[DocumentDistributor]
    doReturn(Future.successful(())).when(distributor).sendDistributionInformation(any[JValue])
    doReturn(DocumentDistributor.Status(usable = true, Set.empty)).when(distributor).status(any[JValue])

    val indexService = mock[IndexService]
    val bulkResponse = mock[BulkResponse]
    doReturn(Future.successful(bulkResponse)).when(indexService).deleteCurrentIndex(any[List[UUID]])
    doReturn(Future.successful(new IndexResponse())).when(indexService).indexCurrentDocument(any[UUID], any[JValue])

    val errorHandler = mock[ErrorHandler]
    doReturn(Future.successful(())).when(errorHandler).handleError(any[Event], any[Throwable])

    val handler: ActorRef = TestActorRef(Props(
      new MessageHandler(config, documentDao, distributor, indexService, errorHandler, retryInterval)(testMerge)))

    def bookEvent(json: JValue = JNothing) = {
      implicit object BookJson extends JsonEventBody[JValue] {
        val jsonMediaType = MediaType("application/vnd.blinkbox.books.ingestion.book.metadata.v2+json")
      }
      Event.json(EventHeader("application/vnd.blinkbox.books.ingestion.book.metadata.v2+json"), json)
    }

    def checkNoFailures() = {
      // Check that everything was called at least once.
      verify(documentDao, atLeastOnce()).storeHistoryDocument(any[JValue], any[Boolean])
      verify(documentDao, atLeastOnce()).getDocumentHistory(any[JValue])
      verify(documentDao, atLeastOnce()).storeCurrentDocument(any[JValue], any[Boolean])
      // Check no errors were sent.
      verify(errorHandler, times(0)).handleError(any[Event], any[Throwable])
    }

    /** Check that event processing failed and was treated correctly. */
    def checkFailure[T <: Throwable](event: Event)(implicit manifest: Manifest[T]): Unit = {
      // Check event was passed on to error handler, along with the expected exception.
      val expectedExceptionClass = manifest.runtimeClass.asInstanceOf[Class[T]]
      verify(errorHandler).handleError(eql(event), isA(expectedExceptionClass))
    }
  }
}
