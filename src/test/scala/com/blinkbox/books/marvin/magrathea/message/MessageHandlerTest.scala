package com.blinkbox.books.marvin.magrathea.message

import akka.actor.{ActorRef, ActorSystem, Props, Status}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.marvin.magrathea.SchemaConfig
import com.blinkbox.books.messaging._
import com.blinkbox.books.test.MockitoSyrup
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
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
  implicit def dateTime2JValue(d: DateTime) = JString(ISODateTimeFormat.dateTime().print(d.withZone(DateTimeZone.UTC)))
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
    verify(documentDao, times(1)).storeHistoryDocument(captor.capture())
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
    verify(documentDao, times(1)).storeHistoryDocument(captor.capture())
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
    verify(documentDao, times(1)).storeHistoryDocument(captor.capture())
    val nameHash1 = ((contributors \ "contributors")(1) \ "names" \ "display").sha1
    val ids0: JValue = ("xxx" -> "AA") ~ ("bbb" -> "BB")
    val ids1: JValue = "bbb" -> nameHash1
    val cArray = captor.getValue \ "contributors"
    cArray.children.size shouldEqual 2
    cArray(0) \ "ids" shouldEqual ids0
    cArray(1) \ "ids" shouldEqual ids1
  }

  it should "delete all history lookupKeyMatches except the first" in new TestFixture {
    val kmA, kmB, kmC = lookupKeyMatch()
    doReturn(Future.successful(List(kmA, kmB, kmC))).when(documentDao).lookupHistoryDocument(any[String])
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
    val captor = ArgumentCaptor.forClass(classOf[List[(String, String)]])
    verify(documentDao, times(1)).deleteHistoryDocuments(captor.capture())
    captor.getValue shouldEqual List(
      ((kmB \ "value" \ "_id").extract[String], (kmB \ "value" \ "_rev").extract[String]),
      ((kmC \ "value" \ "_id").extract[String], (kmC \ "value" \ "_rev").extract[String])
    )
  }

  it should "store and override the incoming document if there is at least one lookup match" in new TestFixture {
    val lookupKey = lookupKeyMatch()
    doReturn(Future.successful(List(lookupKey))).when(documentDao).lookupHistoryDocument(any[String])
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
    val captor = ArgumentCaptor.forClass(classOf[JValue])
    verify(documentDao, times(1)).storeHistoryDocument(captor.capture())
    captor.getValue \ "_id" shouldEqual lookupKey \ "value" \ "_id"
    captor.getValue \ "_rev" shouldEqual lookupKey \ "value" \ "_rev"
  }

  it should "store and not override the incoming document if there are no lookup matches" in new TestFixture {
    doReturn(Future.successful(List.empty)).when(documentDao).lookupHistoryDocument(any[String])
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
    val captor = ArgumentCaptor.forClass(classOf[JValue])
    verify(documentDao, times(1)).storeHistoryDocument(captor.capture())
    captor.getValue \ "_id" shouldEqual JNothing
    captor.getValue \ "_rev" shouldEqual JNothing
  }

  it should "delete all latest lookupKeyMatches except the first" in new TestFixture {
    val kmA, kmB, kmC = lookupKeyMatch()
    doReturn(Future.successful(List(kmA, kmB, kmC))).when(documentDao).lookupLatestDocument(any[String])
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
    val captor = ArgumentCaptor.forClass(classOf[List[(String, String)]])
    verify(documentDao, times(1)).deleteLatestDocuments(captor.capture())
    captor.getValue shouldEqual List(
      ((kmB \ "value" \ "_id").extract[String], (kmB \ "value" \ "_rev").extract[String]),
      ((kmC \ "value" \ "_id").extract[String], (kmC \ "value" \ "_rev").extract[String])
    )
  }

  it should "store and override the merged document if there is at least one lookup match" in new TestFixture {
    val lookupKey = lookupKeyMatch()
    doReturn(Future.successful(List(lookupKey))).when(documentDao).lookupLatestDocument(any[String])
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
    val captor = ArgumentCaptor.forClass(classOf[JValue])
    verify(documentDao, times(1)).storeLatestDocument(captor.capture())
    captor.getValue \ "_id" shouldEqual lookupKey \ "value" \ "_id"
    captor.getValue \ "_rev" shouldEqual lookupKey \ "value" \ "_rev"
  }

  it should "store and not override the merged document if there are no lookup matches" in new TestFixture {
    doReturn(Future.successful(List.empty)).when(documentDao).lookupLatestDocument(any[String])
    handler ! bookEvent(sampleBook())
    checkNoFailures()
    expectMsgType[Status.Success]
    val captor = ArgumentCaptor.forClass(classOf[JValue])
    verify(documentDao, times(1)).storeLatestDocument(captor.capture())
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
    verify(documentDao, times(0)).lookupHistoryDocument(any[String])
  }

  it should "recover from a temporary connection failure" in new TestFixture {
    when(documentDao.lookupHistoryDocument(any[String]))
      .thenReturn(Future.failed(new TimeoutException()))
      .thenReturn(Future.failed(new ConnectionException("oops")))
      .thenReturn(Future.successful(List.empty))
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
    val nameHash0 = ((contributors \ "contributors")(0) \ "names" \ "display").sha1
    val nameHash1 = ((contributors \ "contributors")(1) \ "names" \ "display").sha1
    val bookClassification = compact(render(contributors \ "classification"))
    val classification0 = compact(render(JArray(List[JValue](("realm" -> "bbb_id") ~ ("id" -> nameHash0)))))
    val classification1 = compact(render(JArray(List[JValue](("realm" -> "bbb_id") ~ ("id" -> nameHash1)))))
    verify(documentDao, times(1)).fetchHistoryDocuments(config.book, bookClassification)
    verify(documentDao, times(1)).fetchHistoryDocuments(config.contributor, classification0)
    verify(documentDao, times(1)).fetchHistoryDocuments(config.contributor, classification1)
    verify(documentDao, times(3)).storeLatestDocument(any[JValue])
  }

  trait TestFixture {
    val config = mock[SchemaConfig]
    doReturn("ingestion.book.metadata.v2").when(config).book
    doReturn("ingestion.contributor.metadata.v2").when(config).contributor

    val documentDao = mock[DocumentDao]

    private val lookupDocument = List.empty
    doReturn(Future.successful(lookupDocument)).when(documentDao).lookupHistoryDocument(any[String])
    doReturn(Future.successful(lookupDocument)).when(documentDao).lookupLatestDocument(any[String])

    private val fetchHistoryDocuments = List(sampleBook(), sampleBook(), sampleBook())
    doReturn(Future.successful(fetchHistoryDocuments)).when(documentDao).fetchHistoryDocuments(any[String], any[String])

    doReturn(Future.successful(())).when(documentDao).storeLatestDocument(any[JValue])
    doReturn(Future.successful(())).when(documentDao).storeHistoryDocument(any[JValue])
    doReturn(Future.successful(())).when(documentDao).deleteHistoryDocuments(any[List[(String, String)]])
    doReturn(Future.successful(())).when(documentDao).deleteLatestDocuments(any[List[(String, String)]])

    val distributor = mock[DocumentDistributor]
    doReturn(Future.successful(())).when(distributor).sendDistributionInformation(any[JValue])

    val errorHandler = mock[ErrorHandler]
    doReturn(Future.successful(())).when(errorHandler).handleError(any[Event], any[Throwable])

    val handler: ActorRef = TestActorRef(Props(
      new MessageHandler(config, documentDao, distributor, errorHandler, retryInterval)(testMerge)))

    def sampleBook(extraContent: JValue = JNothing): JValue = {
      ("$schema" -> config.book) ~
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
      verify(documentDao, atLeastOnce()).lookupHistoryDocument(any[String])
      verify(documentDao, atLeastOnce()).storeHistoryDocument(any[JValue])
      verify(documentDao, atLeastOnce()).fetchHistoryDocuments(any[String], any[String])
      verify(documentDao, atLeastOnce()).lookupLatestDocument(any[String])
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
