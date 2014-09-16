package com.blinkbox.books.marvin.magrathea.message

import akka.actor.{ActorRef, ActorSystem, Props, Status}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.messaging._
import com.blinkbox.books.test.MockitoSyrup
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.JsonAST.{JNothing, JString, JValue}
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
  implicit def dateTime2JValue(d: DateTime) = JString(ISODateTimeFormat.dateTime().print(d.withZone(DateTimeZone.UTC)))
  val retryInterval = 100.millis

  override protected def afterAll(): Unit = system.shutdown()

  "A message handler" should "handle a book message" in new TestFixture {
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
  }

  it should "delete all lookupKeyMatches except the first" in new TestFixture {
    doReturn(Future.successful(List(lookupKeyMatch(), lookupKeyMatch(), lookupKeyMatch()))).when(documentDao).lookupDocument(any[String])
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
    val captor = ArgumentCaptor.forClass(classOf[List[(String, String)]])
    verify(documentDao, times(1)).deleteDocuments(captor.capture())
    captor.getValue.size shouldEqual 2
  }

  it should "store and override the incoming document if there is at least one lookup match" in new TestFixture {
    val lookupKey = lookupKeyMatch()
    doReturn(Future.successful(List(lookupKey))).when(documentDao).lookupDocument(any[String])
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
    val captor = ArgumentCaptor.forClass(classOf[JValue])
    verify(documentDao, times(1)).storeHistoryDocument(captor.capture())
    captor.getValue \ "_id" shouldEqual lookupKey \ "value" \ "_id"
    captor.getValue \ "_rev" shouldEqual lookupKey \ "value" \ "_rev"
  }

  it should "store and not override the incoming document if there are no lookup matches" in new TestFixture {
    doReturn(Future.successful(List.empty)).when(documentDao).lookupDocument(any[String])
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
    val captor = ArgumentCaptor.forClass(classOf[JValue])
    verify(documentDao, times(1)).storeHistoryDocument(captor.capture())
    captor.getValue \ "_id" shouldEqual JNothing
    captor.getValue \ "_rev" shouldEqual JNothing
  }

  it should "merge even if the history has only one document" in new TestFixture {
    doReturn(Future.successful(List(sampleBook()))).when(documentDao).fetchHistoryDocuments(any[String], any[String])
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
  }

  it should "not try to merge an empty history list" in new TestFixture {
    val event = bookEvent(sampleBook())
    doReturn(Future.successful(List.empty)).when(documentDao).fetchHistoryDocuments(any[String], any[String])
    handler ! event
    checkFailure[IllegalArgumentException](event)
    expectMsgType[Status.Success]
  }

  it should "fail if the lookup key cannot be extracted" in new TestFixture {
    val event = bookEvent("whatever" -> 5)
    handler ! event
    checkFailure[IllegalArgumentException](event)
    expectMsgType[Status.Success]
    verify(documentDao, times(0)).lookupDocument(any[String])
  }

  it should "recover from a temporary connection failure" in new TestFixture {
    when(documentDao.lookupDocument(any[String]))
      .thenReturn(Future.failed(new TimeoutException()))
      .thenReturn(Future.failed(new ConnectionException("oops")))
      .thenReturn(Future.successful(List.empty))
    handler ! bookEvent(sampleBook())
    expectMsgType[Status.Success]
    checkNoFailures()
  }

  trait TestFixture {
    val errorHandler = mock[ErrorHandler]
    doReturn(Future.successful(())).when(errorHandler).handleError(any[Event], any[Throwable])

    val documentDao = mock[DocumentDao]

    private val lookupDocument = List.empty
    doReturn(Future.successful(lookupDocument)).when(documentDao).lookupDocument(any[String])

    private val fetchHistoryDocuments = List(sampleBook(), sampleBook(), sampleBook())
    doReturn(Future.successful(fetchHistoryDocuments)).when(documentDao).fetchHistoryDocuments(any[String], any[String])

    doReturn(Future.successful(())).when(documentDao).storeLatestDocument(any[JValue])
    doReturn(Future.successful(())).when(documentDao).storeHistoryDocument(any[JValue])
    doReturn(Future.successful(())).when(documentDao).deleteDocuments(any[List[(String, String)]])

    val handler: ActorRef = TestActorRef(Props(new MessageHandler(documentDao, errorHandler, retryInterval)))

    def sampleBook(extraContent: JValue = JNothing): JValue = {
      ("$schema" -> "ingestion.book.metadata.v2") ~
      ("classification" -> List(("realm" -> "isbn") ~ ("id" -> "9780111222333"))) ~
      ("source" ->
        ("$remaining" ->
          ("system" -> ("name" -> "marvin/design_docs") ~ ("version" -> "1.0.0")) ~
          ("role" -> "publisher_ftp") ~
          ("username" -> "jp-publishing") ~
          ("deliveredAt" -> DateTime.now) ~
          ("processedAt" -> DateTime.now)
        )
      ) merge extraContent
    }

    def lookupKeyMatch(extraContent: JValue = JNothing): JValue = {
      ("id" -> "7e93a26396bde6994ccefaf3da003659") ~
      ("key" -> List("whatever-does-not-matter")) ~
      ("value" -> ("_id" -> "7e93a26396bde6994ccefaf3da003659") ~ ("_rev" -> "1-da5a08470ffe6bca22174a02f5fd5714"))
    }

    def bookEvent(json: JValue = JNothing) = {
      implicit object BookJson extends JsonEventBody[JValue] {
        val jsonMediaType = MediaType("application/vnd.blinkbox.books.ingestion.book.metadata.v2+json")
      }
      Event.json(EventHeader("application/vnd.blinkbox.books.ingestion.book.metadata.v2+json"), json)
    }

    def checkNoFailures() = {
      // Check that everything was called at least once.
      verify(documentDao, atLeastOnce()).lookupDocument(any[String])
      verify(documentDao, atLeastOnce()).storeHistoryDocument(any[JValue])
      verify(documentDao, atLeastOnce()).fetchHistoryDocuments(any[String], any[String])
      verify(documentDao, atLeastOnce()).storeLatestDocument(any[JValue])
      // Check no errors were sent.
      verify(errorHandler, times(0)).handleError(any[Event], any[Throwable])
    }

    /** Check that event processing failed and was treated correctly. */
    def checkFailure[T <: Throwable](event: Event)(implicit manifest: Manifest[T]) {
      // Check event was passed on to error handler, along with the expected exception.
      val expectedExceptionClass = manifest.runtimeClass.asInstanceOf[Class[T]]
      verify(errorHandler).handleError(eql(event), isA(expectedExceptionClass))
    }
  }
}
