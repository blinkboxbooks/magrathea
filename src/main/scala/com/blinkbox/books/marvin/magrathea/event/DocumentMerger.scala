package com.blinkbox.books.marvin.magrathea.event

import akka.actor.{Actor, ActorPath, ActorRef, Props}
import akka.pattern.PipeToSupport
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.marvin.magrathea.event.DocumentMerger._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.Future

object DocumentMerger {
  case class MergeDocuments(docs: List[JValue])
  case class DocumentResult(doc: JValue)
  private case class MergeJob(key: Int, docs: List[JValue])
  private case class MergeResult(key: Int, doc: JValue)
}

class DocumentMerger(resultReceiver: ActorRef)(merge: (JValue, JValue) => JValue)
  extends Actor with StrictLogging with Json4sJacksonSupport with JsonMethods {

  implicit val ec = context.dispatcher
  implicit val json4sJacksonFormats = DefaultFormats
  // a map maintaining whether a job has been completed
  private val maxDocsPerJob = 10
  private val numOfWorkers = 4
  private val masterName = "master"

  // create a Master along with its Workers
  val master = context.actorOf(Props[Master], masterName)
  1 to numOfWorkers foreach(_ => createWorker(masterName))

  var pendingJobs = Set.empty[Int]
  var merged = List.empty[JValue]

  override def receive: Receive = {
    case MergeDocuments(docs) =>
      docs.grouped(maxDocsPerJob).foreach { docs =>
        val key = docs.hashCode()
        master ! MergeJob(key, docs)
        pendingJobs += key
      }
    case MergeResult(key, doc) =>
      pendingJobs -= key
      merged +:= doc
      if (merged.size == 1 && pendingJobs.isEmpty)
        merged.headOption.foreach(resultReceiver ! DocumentResult(_))
      else if (merged.size >= maxDocsPerJob || pendingJobs.isEmpty) {
        if (merged.size > maxDocsPerJob)
          logger.warn(s"Queue size is ${merged.size} (> maxDocsPerJob -- $maxDocsPerJob)")
        self ! MergeDocuments(merged)
        merged = List.empty
      }
  }

  private def createWorker(master: String) = context.actorOf(Props(new MergeWorker(ActorPath.fromString(
    "akka://%s/user/%s/%s".format(context.system.name, self.path.name, master)))))

  private class MergeWorker(masterLocation: ActorPath) extends Worker(masterLocation) with PipeToSupport {
    implicit val ec = context.dispatcher

    override def doWork(workSender: ActorRef, msg: Any): Unit = {
      Future {
        msg match {
          case MergeJob(key, docs) =>
            val doc = docs.reduceLeft(merge)
            workSender ! MergeResult(key, doc)
            WorkComplete(doc)
          case _ =>
        }
      } pipeTo self
    }
  }
}

object MergeWorker {
  def merge(a: JValue, b: JValue): JValue = {
    a merge b
  }
}
