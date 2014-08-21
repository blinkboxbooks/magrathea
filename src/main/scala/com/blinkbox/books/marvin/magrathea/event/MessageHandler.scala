package com.blinkbox.books.marvin.magrathea.event

import java.io.IOException
import java.net.URL

import akka.actor.ActorRef
import akka.util.Timeout
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.messaging.{ErrorHandler, Event, ReliableEventHandler}
import org.json4s.JsonAST._
import org.json4s.native.JsonMethods._
import spray.client.pipelining._
import spray.http.StatusCodes._
import spray.http._
import spray.httpx.Json4sJacksonSupport

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, TimeoutException}

class MessageHandler(couchdbUrl: URL, errorHandler: ErrorHandler, retryInterval: FiniteDuration)
  extends ReliableEventHandler(errorHandler, retryInterval) with Json4sJacksonSupport {

  implicit val timeout = Timeout(retryInterval)
  implicit val json4sJacksonFormats = DefaultFormats

  override protected def handleEvent(event: Event, originalSender: ActorRef) = Future {
    val documentJson = parse(event.body.asString())
    val key = compact(render(extractDocumentKey(documentJson)))

    // check if the document key exists
    val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
    val lookupUri = Uri(s"$couchdbUrl/history/_design/index/_view/replace_lookup")
    pipeline(Get(lookupUri.withQuery(("key", key)))).map { r =>
      val respJson = parse(r.entity.asString)
      val foundDocs = respJson \ "rows"
      if (foundDocs.children.size > 0) {
        // TODO: delete if there are more documents
        println(s"there are ${foundDocs.children.size} documents with this id")
        // replace it
        val idRev = (respJson \ "rows")(0) \ "value"
        val finalJson = idRev merge documentJson
        pipeline(Post(s"$couchdbUrl/history", finalJson)).map { r =>
          val status = r.status
          if (status == Created) {
            println("Document replaced!")
          } else {
            println("An error occurred while replacing: " + status)
          }
        }
      } else {
        // store it
        pipeline(Post(s"$couchdbUrl/history", documentJson)).map { r =>
          val status = r.status
          if (status == Created) {
            println("Document stored!")
          } else {
            println("An error occurred while storing: " + status)
          }
        }
      }
    }
  }

  // Consider the error temporary if the exception or its root cause is an IO exception or timeout.
  @tailrec
  final override protected def isTemporaryFailure(e: Throwable) = e.isInstanceOf[IOException] ||
    e.isInstanceOf[TimeoutException] || Option(e.getCause).isDefined && isTemporaryFailure(e.getCause)

  private def extractDocumentKey(json: JValue): JArray = {
    val schema = json \ "$schema"
    val remaining = (json \ "source" \ "$remaining")
      .removeField(_._1 == "processedAt").removeField(_._1 == "system")
    val classification = json \ "classification"
    JArray(List(schema, remaining, classification))
  }
}
