package com.blinkbox.books.marvin.magrathea.event

import java.io.IOException
import java.net.URL

import akka.actor.ActorRef
import akka.util.Timeout
import com.blinkbox.books.marvin.magrathea.{AuthorImage, Realm, Role, UriType}
import com.blinkbox.books.messaging.{ErrorHandler, Event, ReliableEventHandler}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.json4s._
import org.json4s.ext._
import org.json4s.native.JsonMethods._
import spray.client.pipelining._
import spray.http._
import spray.httpx.Json4sJacksonSupport

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, TimeoutException}

class AuthorImageMessageHandler(maestro: ActorRef, errorHandler: ErrorHandler, retryInterval: FiniteDuration)
  extends ReliableEventHandler(errorHandler, retryInterval) with Json4sJacksonSupport {

  implicit val timeout = Timeout(retryInterval)
  implicit val json4sJacksonFormats = DefaultFormats + ISODateTimeSerializer + URLSerializer +
    new EnumNameSerializer(Realm) + new EnumNameSerializer(Role) + new EnumNameSerializer(UriType)

  case object ISODateTimeSerializer extends CustomSerializer[DateTime](_ => ( {
    case JString(s) => ISODateTimeFormat.dateTime().parseDateTime(s)
    case JNull => null
  }, {
    case d: DateTime => JString(ISODateTimeFormat.dateTime().print(d))
  }))

  case object URLSerializer extends CustomSerializer[URL](_ => ( {
    case JString(s) => new URL(s)
    case JNull => null
  }, {
    case u: URL => JString(u.toString)
  }))

  override protected def handleEvent(event: Event, originalSender: ActorRef) = Future {
    log.info("Received: " + event.body.asString())
    val json = event.body.asString()
    val authorImage = parse(json).extract[AuthorImage]
    log.info("JSON verified successfully: " + authorImage)
    log.info("Storing to CouchDB...")
    storeToCouch(authorImage)
  }

  private def storeToCouch(authorImage: AuthorImage) {
    val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
    pipeline(Post("http://localhost:5984/magrathea", authorImage)).map { r =>
      log.info(s"Response code: ${r.status.intValue}")
    }
  }

  // Consider the error temporary if the exception or its root cause is an IO exception or timeout.
  @tailrec
  final override protected def isTemporaryFailure(e: Throwable) = e.isInstanceOf[IOException] ||
    e.isInstanceOf[TimeoutException] || Option(e.getCause).isDefined && isTemporaryFailure(e.getCause)
}
