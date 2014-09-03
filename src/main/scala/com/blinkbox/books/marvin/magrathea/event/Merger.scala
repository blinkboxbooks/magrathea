package com.blinkbox.books.marvin.magrathea.event

import akka.actor.{Actor, ActorPath, ActorRef, Props}
import akka.pattern.PipeToSupport
import com.blinkbox.books.marvin.magrathea.MergerConfig
import com.blinkbox.books.marvin.magrathea.event.Merger._
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.concurrent.Future

object Merger {
  case class MergeRequest[T](docs: List[T])
  case class MergeResponse[T](doc: T)
  private case class MergeJob[T](key: Int, docs: List[T])
  private case class MergeResult[T](key: Int, doc: T)
}

class Merger[T](config: MergerConfig, resultReceiver: ActorRef)(merge: (T, T) => T)
  extends Actor with StrictLogging {

  require(config.maxDocsPerJob > 1)
  require(config.numOfWorkers > 0)

  implicit val ec = context.dispatcher
  private val masterName = "master"

  // create a Master along with its Workers
  val master = context.actorOf(Props[Master], masterName)
  1 to config.numOfWorkers foreach(_ => createWorker(masterName))

  var pendingJobs = Set.empty[Int]
  var merged = List.empty[T]

  override def receive: Receive = {
    case msg: MergeRequest[T] =>
      msg.docs.grouped(config.maxDocsPerJob).foreach(docs => sendMergeJob(docs))
    case msg: MergeResult[T] =>
      pendingJobs -= msg.key
      merged +:= msg.doc
      if (merged.size == 1 && pendingJobs.isEmpty)
        merged.headOption.foreach(r => resultReceiver ! MergeResponse(r))
      else if (merged.size >= config.maxDocsPerJob || pendingJobs.isEmpty) {
        if (merged.size > config.maxDocsPerJob)
          logger.warn(s"Queue size is ${merged.size} (> maxDocsPerJob -- ${config.maxDocsPerJob})")
        sendMergeJob(merged)
        merged = List.empty
      }
  }

  private def sendMergeJob(docs: List[T]): Unit = {
    val key = docs.hashCode()
    master ! MergeJob(key, docs)
    pendingJobs += key
  }

  private def createWorker(master: String) = context.actorOf(Props(new MergeWorker(ActorPath.fromString(
    "akka://%s/user/%s/%s".format(context.system.name, self.path.name, master)))))

  private class MergeWorker(masterLocation: ActorPath) extends Worker(masterLocation) with PipeToSupport {
    implicit val ec = context.dispatcher

    override def doWork(workSender: ActorRef, msg: Any): Unit = {
      Future {
        msg match {
          case msg: MergeJob[T] =>
            val doc = msg.docs.reduceLeft(merge)
            workSender ! MergeResult(msg.key, doc)
            WorkComplete(doc)
          case _ =>
        }
      } pipeTo self
    }
  }
}
