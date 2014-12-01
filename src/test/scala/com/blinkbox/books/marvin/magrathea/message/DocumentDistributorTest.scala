package com.blinkbox.books.marvin.magrathea.message

import com.blinkbox.books.marvin.magrathea.message.DocumentDistributor._
import com.blinkbox.books.marvin.magrathea.{DistributorConfig, SchemaConfig, TestHelper}
import com.blinkbox.books.test.MockitoSyrup
import org.json4s.JsonAST.JValue
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, Matchers}

@RunWith(classOf[JUnitRunner])
class DocumentDistributorTest extends FlatSpecLike with MockitoSyrup with Matchers with TestHelper {

  behavior of "The document distributor"

  it should "distribute a book which respects distribution business logic" in new TestFixture {
    val status = distributor.status(distBook())
    status.sellable shouldEqual true
    status.reasons shouldEqual None
  }

  it should "not distribute a book without a title" in new TestFixture {
    val status = distributor.status(distBook(noTitle = true))
    shouldNotBeSellableWith(status, Reason.NoTitle)
  }

  it should "not distribute a book marked as unavailable by publisher" in new TestFixture {
    val status = distributor.status(distBook(unavailable = true))
    shouldNotBeSellableWith(status, Reason.Unavailable)
  }

  it should "not distribute a book without supply rights" in new TestFixture {
    val status = distributor.status(distBook(unsuppliable = true))
    shouldNotBeSellableWith(status, Reason.Unsuppliable)
  }

  it should "not distribute a book without sales rights" in new TestFixture {
    val status = distributor.status(distBook(unsellable = true))
    shouldNotBeSellableWith(status, Reason.Unsellable)
  }

  it should "not distribute a book without a publisher" in new TestFixture {
    val status = distributor.status(distBook(noPublisher = true))
    shouldNotBeSellableWith(status, Reason.NoPublisher)
  }

  it should "not distribute a book without a cover" in new TestFixture {
    val status = distributor.status(distBook(noCover = true))
    shouldNotBeSellableWith(status, Reason.NoCover)
  }

  it should "not distribute a book without an ePub" in new TestFixture {
    val status = distributor.status(distBook(noEpub = true))
    shouldNotBeSellableWith(status, Reason.NoEpub)
  }

  it should "not distribute a book without english in languages" in new TestFixture {
    val status = distributor.status(distBook(notEnglish = true))
    shouldNotBeSellableWith(status, Reason.NotEnglish)
  }

  it should "not distribute a book without a description" in new TestFixture {
    val status = distributor.status(distBook(noDescription = true))
    shouldNotBeSellableWith(status, Reason.NoDescription)
  }

  it should "not distribute a book without a usable price" in new TestFixture {
    val status = distributor.status(distBook(noUsablePrice = true))
    shouldNotBeSellableWith(status, Reason.NoUsablePrice)
  }

  it should "not distribute a book with racy titles" in new TestFixture {
    val status = distributor.status(distBook(racy = true))
    shouldNotBeSellableWith(status, Reason.Racy)
  }

  it should "not distribute a book without title, ePub and description" in new TestFixture {
    val status = distributor.status(distBook(noTitle = true, noEpub = true, noDescription = true))
    shouldNotBeSellableWith(status, Reason.NoTitle, Reason.NoEpub, Reason.NoDescription)
  }

  trait TestFixture extends TestHelper {
    val config = mock[DistributorConfig]
    val schemas = mock[SchemaConfig]
    doReturn("ingestion.book.metadata.v2").when(schemas).book
    doReturn("ingestion.contributor.metadata.v2").when(schemas).contributor

    val distributor = new DocumentDistributor(config, schemas)

    def distBook(noTitle: Boolean = false, unavailable: Boolean = false, unsuppliable: Boolean = false,
      unsellable: Boolean = false, noPublisher: Boolean = false, noCover: Boolean = false,
      noEpub: Boolean = false, notEnglish: Boolean = false, noDescription: Boolean = false,
      noUsablePrice: Boolean = false, racy: Boolean = false): JValue = {
      val book = sampleBook()
      // TODO: Logic for generating test cases here
      book
    }

    def shouldNotBeSellableWith(status: Status, reason: Reason.Value*): Unit = {
      status.sellable shouldEqual false
      status.reasons shouldEqual Some(reason)
    }
  }
}
