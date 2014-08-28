package com.blinkbox.books.marvin.magrathea.event

import akka.actor.{Props, Actor, ActorPath, ActorRef}
import akka.pattern.PipeToSupport
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.EventListenerConfig
import com.blinkbox.books.marvin.magrathea.event.DocumentMerger.{DocumentKey, MergeDocuments}
import com.blinkbox.books.spray._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods
import spray.client.pipelining._
import spray.http.Uri.Path
import spray.http.{HttpRequest, HttpResponse, Uri}
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.Future

object DocumentMerger {
  case class DocumentKey(key: String, schema: String)
  case class MergeDocuments(a: JValue, b: JValue)
}

class DocumentMerger(config: EventListenerConfig)
  extends Actor with Json4sJacksonSupport with JsonMethods with StrictLogging {

  implicit val ec = context.dispatcher
  implicit val json4sJacksonFormats = DefaultFormats

  private val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
  private val bookUri = config.couchdbUrl.withPath(config.couchdbUrl.path ++ Path("/history/_design/history/_view/book"))
  private val contributorUri = config.couchdbUrl.withPath(config.couchdbUrl.path ++ Path("/history/_design/history/_view/contributor"))

  override def receive: Receive = {
    case k @ DocumentKey(_, _) =>
      pipeline(Get(getCouchQuery(k))).map { r =>
        val respJson = parse(r.entity.asString)
        val docsCount = (respJson \ "rows").children.size
        // create master
        val m = context.actorOf(Props[Master], "master")
        // create workers
        createWorker("master")
        createWorker("master")
        createWorker("master")
        createWorker("master")
        // send some work to the master
//        var docs = (respJson \ "rows").children
//        while (docs.size) {
//
//        }
//        docs.take(2).foldLeft()
//        for (x <- (respJson \ "rows").children) {
//          x \ "value"
//        }
      }
    case x => logger.error("Received unknown message: {}", x.toString)
  }

  private def getCouchQuery(docKey: DocumentKey): Uri = {
    if (docKey.schema == config.book.schema) {
      bookUri.withQuery(("key", docKey.key))
    } else if (docKey.schema == config.contributor.schema) {
      contributorUri.withQuery(("key", docKey.key))
    } else {
      throw new IllegalArgumentException(s"Unsupported schema: ${docKey.schema}")
    }
  }

  private def createWorker(name: String) = context.actorOf(Props(
    new MergeWorker(ActorPath.fromString(
      "akka://%s/user/%s".format(context.system.name, name)))))
}

class MergeWorker(masterLocation: ActorPath) extends Worker(masterLocation) with PipeToSupport {
  implicit val ec = context.dispatcher

  override def doWork(workSender: ActorRef, msg: Any): Unit = {
    Future {
      msg match {
        case MergeDocuments(a, b) => WorkComplete(a merge b)
        case _ =>
      }
    } pipeTo self
  }
}