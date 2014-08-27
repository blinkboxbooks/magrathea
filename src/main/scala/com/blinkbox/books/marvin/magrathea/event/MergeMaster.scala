package com.blinkbox.books.marvin.magrathea.event

import java.net.URL

import akka.actor.Actor
import com.blinkbox.books.marvin.magrathea.event.MergeMaster.DocumentKey
import com.typesafe.scalalogging.slf4j.StrictLogging
import spray.client.pipelining._
import spray.http.Uri.Path
import spray.http.{Uri, HttpRequest, HttpResponse}
import com.blinkbox.books.spray._

import scala.concurrent.Future

object MergeMaster {
  case class DocumentKey(key: String, msgType: String)
}

class MergeMaster(couchdbUrl: URL) extends Actor with StrictLogging {

  implicit val ec = context.dispatcher

  private val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
  private val bookUri = couchdbUrl.withPath(couchdbUrl.path ++ Path("/history/_design/history/_view/book"))
  private val contributorUri = couchdbUrl.withPath(couchdbUrl.path ++ Path("/history/_design/history/_view/contributor"))

  override def receive = {
    case k @ DocumentKey(key, msgType) =>
      println("Query to CouchDB: " + getCouchQuery(k))
    case x => logger.error("Received unknown message: {}", x.toString)
  }

  private def getCouchQuery(docKey: DocumentKey): Uri = {
    if (docKey.msgType == "ingestion.book.metadata.v2") {
      contributorUri.withQuery(("key", docKey.key))
    } else {
      bookUri.withQuery(("key", docKey.key))
    }
  }
}

class MergeSlave extends Actor with StrictLogging {
  override def receive = {
    case x => logger.info("Received {}", x.toString)
  }
}
