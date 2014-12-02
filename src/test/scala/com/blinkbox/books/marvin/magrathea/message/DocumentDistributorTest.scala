package com.blinkbox.books.marvin.magrathea.message

import com.blinkbox.books.marvin.magrathea.message.DocumentDistributor._
import com.blinkbox.books.marvin.magrathea.{DistributorConfig, SchemaConfig, TestHelper}
import com.blinkbox.books.test.MockitoSyrup
import org.json4s.JsonAST.{JNothing, JValue}
import org.json4s.JsonDSL._
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
      val title: JValue = if (noTitle) JNothing else "title" -> "a title"
      val availability: JValue = "availability" ->
        ("availabilityCode" -> ("code" -> "NP") ~ ("available" -> !unavailable)) ~
        ("notificationType" -> ("code" -> "02") ~ ("available" -> !unavailable))
      val suppliable: JValue = "supplyRights" -> ("WORLD" -> !unsuppliable)
      val sellable: JValue = "salesRights" -> ("WORLD" -> !unsellable)
      val publisher: JValue = if (noPublisher) JNothing else
        ("publisher" -> "Worthy Publishing") ~ ("imprint" -> "Worthy Publishing")
      val cover: JValue = if (noCover) JNothing else "images" -> List(
        ("classification" -> List(
          ("realm" -> "type") ~
          ("id" -> "front_cover")
        )) ~
        ("token" -> "bbbmap:covers:mnop3456") ~
        ("width" -> 1200) ~
        ("height" -> 2500) ~
        ("size" -> 15485)
      )
      val epub: JValue = if (noEpub) JNothing else "media" -> ("epubs" ->
        ("best" -> "") ~
        ("items" -> List(
          ("classification" -> List(
            ("realm" -> "epub_id") ~ ("id" -> "abc1234"),
            ("realm" -> "type") ~ ("id" -> "full_bbbdrm")
          )) ~
          ("token" -> "bbbmap:epub-encrypted:abcd1234") ~
          ("keyId" -> "987654321QWERTYUIOP") ~
          ("wordCount" -> 37462) ~
          ("size" -> 254850),
          ("classification" -> List(
            ("realm" -> "epub_id") ~ ("id" -> "abc1234"),
            ("realm" -> "type") ~ ("id" -> "sample")
          )) ~
          ("token" -> "bbbmap:epub-sample:efgh5678") ~
          ("wordCount" -> 3746) ~
          ("size" -> 25485),
          ("classification" -> List(
            ("realm" -> "epub_id") ~ ("id" -> "efgh5678"),
            ("realm" -> "type") ~ ("id" -> "origin")
          )) ~
          ("token" -> "bbbmap:publishers:ijkl9012") ~
          ("wordCount" -> 37462) ~
          ("size" -> 254850)
        ))
      )
      val english: JValue = if (notEnglish) JNothing else "languages" -> List("eng")
      val description: JValue = if (noDescription) JNothing else "descriptions" -> (
        ("best" -> List(
          ("realm" -> "onix-codelist-33") ~ ("id" -> "03")
        )) ~
        ("items" -> List(
          ("classification" -> List(
            ("realm" -> "onix-codelist-33") ~ ("id" -> "03")
          )) ~
          ("type" -> "03") ~
          ("content" -> "Blah blah")
        ))
      )
      val price: JValue = if (noUsablePrice) JNothing else "prices" -> List(
        ("tax" -> List.empty) ~
        ("amount" -> 21.99) ~
        ("currency" -> "GBP") ~
        ("isAgency" -> false) ~
        ("includesTax" -> false) ~
        ("discountRate" -> 0.525) ~
        ("applicableRegions" -> "")
      )
      val racyField: JValue = if (!racy) JNothing else "subjects" -> List(
        ("code" -> "REL012010") ~ ("type" -> "BISAC")
      )
      sampleBook(
        ("isbn" -> "9780111222333") ~
        ("dates" -> List("publish" -> "2013-03-01")) ~
        ("format" -> ("epubType" -> "029") ~ ("productForm" -> "DG") ~ ("marvinIncompatible" -> false)) ~
        ("media" -> List.empty)
        merge title merge availability merge suppliable merge sellable merge publisher merge cover
          merge epub merge english merge description merge price merge racyField
      )
    }

    def shouldNotBeSellableWith(status: Status, reason: Reason.Value*): Unit = {
      status.sellable shouldEqual false
      status.reasons shouldEqual Some(reason)
    }
  }
}
