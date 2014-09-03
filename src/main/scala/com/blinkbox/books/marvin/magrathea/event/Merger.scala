package com.blinkbox.books.marvin.magrathea.event

import akka.actor.{Actor, ActorPath, ActorRef, Props}
import akka.pattern.PipeToSupport
import com.blinkbox.books.marvin.magrathea.MergerConfig
import com.blinkbox.books.marvin.magrathea.event.Merger._
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.concurrent.Future

object Merger {
  case class MergeRequest[T](items: List[T])
  case class MergeResponse[T](item: T)
  private case class MergeJob[T](key: Int, items: List[T])
  private case class MergeResult[T](key: Int, item: T)
}

class Merger[T](config: MergerConfig, resultReceiver: ActorRef)(merge: (T, T) => T)
  extends Actor with StrictLogging {

  require(config.maxItemsPerJob > 1)
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
      msg.items.grouped(config.maxItemsPerJob).foreach(items => sendMergeJob(items))
    case msg: MergeResult[T] =>
      pendingJobs -= msg.key
      merged +:= msg.item
      if (merged.size == 1 && pendingJobs.isEmpty)
        merged.headOption.foreach(r => resultReceiver ! MergeResponse(r))
      else if (merged.size >= config.maxItemsPerJob || pendingJobs.isEmpty) {
        if (merged.size > config.maxItemsPerJob)
          logger.warn(s"Queue size is ${merged.size} (> maxItemsPerJob -- ${config.maxItemsPerJob})")
        sendMergeJob(merged)
        merged = List.empty
      }
  }

  private def sendMergeJob(items: List[T]): Unit = {
    val key = items.hashCode()
    master ! MergeJob(key, items)
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
            val item = msg.items.reduceLeft(merge)
            workSender ! MergeResult(msg.key, item)
            WorkComplete(item)
          case _ =>
        }
      } pipeTo self
    }
  }
}
