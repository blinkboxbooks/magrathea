package com.blinkbox.books.marvin.magrathea.event

import akka.actor.{Props, Actor, ActorRef}
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.event.DocumentMerger._
import com.blinkbox.books.marvin.magrathea.event.MergeWorker._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods
import spray.httpx.Json4sJacksonSupport

import scala.collection.mutable

object DocumentMerger {
  case class MergeDocuments(docs: List[JValue])
}

class DocumentMerger extends Actor with StrictLogging with Json4sJacksonSupport with JsonMethods {
  implicit val ec = context.dispatcher
  implicit val json4sJacksonFormats = DefaultFormats
  private val docsQ = mutable.Queue.empty[(JValue, Any)]
  private val maxJobs = 5

  override def receive: Receive = {
    case MergeDocuments(docs) =>
      createWorker ! MergeRequest(docs)
    case MergeResult(doc) => println(compact(render(doc)))
    case _ =>
  }

  private def createWorker = context.actorOf(Props(new MergeWorker(self, maxJobs)))
}

object MergeWorker {
  case class MergeRequest(docs: List[JValue])
  case class MergeResult(doc: JValue)
}

class MergeWorker(parent: ActorRef, maxJobs: Int) extends Actor
  with StrictLogging with Json4sJacksonSupport with JsonMethods {

  implicit val ec = context.dispatcher
  implicit val json4sJacksonFormats = DefaultFormats

  override def receive = {
    case MergeRequest(docs) =>
      if (docs.size <= maxJobs) {
        // do the merge
        val merged = docs.reduceLeft { (prev, curr) =>
          prev ++ curr
        }
        parent ! MergeResult(merged)
      } else {
        // divide the merge and spawn other workers
        var jobs = mutable.ListBuffer(docs: _*)
        val jobsPerWorker = Math.ceil(docs.size.toDouble / maxJobs).toInt
        for (i <- 1 to maxJobs) {
          val w = createWorker
          jobs.take(jobsPerWorker)
        }
      }
    case res: MergeResult => parent ! res
    case _ =>
  }

  private def createWorker = context.actorOf(Props(new MergeWorker(self, maxJobs)))
}