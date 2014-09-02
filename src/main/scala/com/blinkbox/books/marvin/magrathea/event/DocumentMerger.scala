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
  private case class MergeJob(key: Int, docs: List[JValue])
  private case class MergeResult(key: Int, doc: JValue)
  private object CheckForFurtherMerging
}

class DocumentMerger extends Actor with StrictLogging with Json4sJacksonSupport with JsonMethods {
  implicit val ec = context.dispatcher
  implicit val json4sJacksonFormats = DefaultFormats
  // a map maintaining whether a job has been completed
  private var jobs = Map.empty[Int, Boolean]
  private var merged = List.empty[JValue]
  private val maxJobsPerWorker = 10
  private val numOfWorkers = 4
  private val masterName = "master"

  // create a Master along with its Workers
  val master = context.actorOf(Props[Master], masterName)
  1 to numOfWorkers foreach(_ => createWorker(masterName))

  override def receive: Receive = {
    case MergeDocuments(docs) =>
      println("Will be merging " + docs.size + " documents...")
      docs.grouped(maxJobsPerWorker).foreach { docs =>
        val key = docs.hashCode()
        master ! MergeJob(key, docs)
        jobs += (key -> false)
        println("added job of " + docs.size + " documents: " + key)
      }
    case MergeResult(key, doc) =>
      println("done with " + key)
      jobs += (key -> true)
      merged :+= doc
      self ! CheckForFurtherMerging
    case CheckForFurtherMerging =>
      val finishedJobs = jobs.count { case (_, finished) => finished }
      val unfinishedJobs = jobs.size - finishedJobs
      if (unfinishedJobs == 0) {
        // we have our result!
        println("FINAL RESULT (" + merged.size + ")")
        val result = merged.headOption
        result.foreach(r => println(compact(render(r))))
        // cleanup
        jobs = Map.empty
        merged = List.empty
      } else if (finishedJobs >= maxJobsPerWorker || unfinishedJobs == 0) {
        // we still have work to do...
        val (docs, newMerged) = merged.splitAt(maxJobsPerWorker)
        merged = newMerged
        val key = docs.hashCode()
        master ! MergeJob(key, docs)
        jobs += (key -> false)
        println("re-added job of " + docs.size + " documents: " + key)
      } else {
        // we cannot merge -- waiting for more documents...
        println("waiting for more documents... currently: " + merged.size)
      }
    case _ =>
  }

  private def createWorker(master: String): ActorRef = context.actorOf(Props(new MergeWorker(
    ActorPath.fromString("akka://%s/user/document-merger/%s".format(context.system.name, master)))))

  class MergeWorker(masterLocation: ActorPath) extends Worker(masterLocation) with PipeToSupport {
    implicit val ec = context.dispatcher

    override def doWork(workSender: ActorRef, msg: Any): Unit = {
      Future {
        msg match {
          case MergeJob(key, docs) =>
            val doc = docs.reduceLeft(mergeHash)
            workSender ! MergeResult(key, doc)
            WorkComplete(doc)
          case _ =>
        }
      } pipeTo self
    }

    private def mergeHash(a: JValue, b: JValue): JValue = {
      a merge b
    }
  }
}
