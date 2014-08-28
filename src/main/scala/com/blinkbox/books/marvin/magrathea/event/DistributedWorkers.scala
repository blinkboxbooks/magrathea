/**
 * Taken from:
 * http://letitcrash.com/post/29044669086/balancing-workload-across-nodes-with-akka-2
 */
package com.blinkbox.books.marvin.magrathea.event

import akka.actor.{Actor, ActorPath, ActorRef, Terminated}
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.collection.mutable

object MasterWorkerProtocol {
  // Messages from Workers
  case class WorkerCreated(worker: ActorRef)
  case class WorkerRequestsWork(worker: ActorRef)
  case class WorkIsDone(worker: ActorRef)

  // Messages to Workers
  case class WorkToBeDone(work: Any)
  case object WorkIsReady
  case object NoWorkToBeDone
}

class Master extends Actor with StrictLogging {
  import com.blinkbox.books.marvin.magrathea.event.MasterWorkerProtocol._

  // Holds known workers and what they may be working on
  val workers = mutable.Map.empty[ActorRef, Option[(ActorRef, Any)]]
  // Holds the incoming list of work to be done as well
  // as the memory of who asked for it
  val workQ = mutable.Queue.empty[(ActorRef, Any)]

  // Notifies workers that there's work available, provided they're
  // not already working on something
  def notifyWorkers(): Unit = {
    if (workQ.nonEmpty) {
      workers.foreach {
        case (worker, m) if m.isEmpty => worker ! WorkIsReady
        case _ =>
      }
    }
  }

  override def receive: Receive = {
    // Worker is alive. Add him to the list, watch him for
    // death, and let him know if there's work to be done
    case WorkerCreated(worker) =>
      logger.debug("Worker created: {}", worker)
      context.watch(worker)
      workers += (worker -> None)
      notifyWorkers()

    // A worker wants more work. If we know about him, he's not currently
    // doing anything, and we've got something to do, give it to him
    case WorkerRequestsWork(worker) =>
      logger.debug("Worker requests work: {}", worker)
      if (workers.contains(worker)) {
        if (workQ.isEmpty)
          worker ! NoWorkToBeDone
        else if (workers(worker) == None) {
          val (workSender, work) = workQ.dequeue()
          workers += (worker -> Some(workSender -> work))
          // Use the special form of 'tell' that lets us supply the sender
          worker.tell(WorkToBeDone(work), workSender)
        }
      }

    // Worker has completed its work and we can clear it out
    case WorkIsDone(worker) =>
      if (!workers.contains(worker))
        logger.error("Blurgh! {} said it's done work but we didn't know about him", worker)
      else
        workers += (worker -> None)

    // A worker died. If he was doing anything then we need to give it to someone
    // else so we just add it back to the master and let things progress as usual
    case Terminated(worker) =>
      if (workers.contains(worker) && workers(worker) != None) {
        logger.error("Blurgh! {} died while processing {}", worker, workers(worker))
        // Send the work that it was doing back to ourselves for processing
        val (workSender, work) = workers(worker).get
        self.tell(work, workSender)
      }
      workers -= worker

    // Anything other than our own protocol is "work to be done"
    case work =>
      logger.debug("Queueing {}", work.toString)
      workQ.enqueue(sender -> work)
      notifyWorkers()
  }
}

abstract class Worker(masterLocation: ActorPath) extends Actor with StrictLogging {
  import com.blinkbox.books.marvin.magrathea.event.MasterWorkerProtocol._

  // We need to know where the master is
  val master = context.actorSelection(masterLocation)

  // This is how our derivations will interact with us. It
  // allows derivations to complete work asynchronously
  case class WorkComplete(result: Any)

  // Required to be implemented
  def doWork(workSender: ActorRef, work: Any): Unit

  // Notify the Master that we're alive
  override def preStart() = master ! WorkerCreated(self)

  // This is the state we're in when we're working on something.
  // In this state we can deal with messages in a much more
  // reasonable manner
  def working(work: Any): Receive = {
    // Pass... we're already working
    case WorkIsReady =>
    // Pass... we're already working
    case NoWorkToBeDone =>
    // Pass... we shouldn't even get this
    case WorkToBeDone(_) =>
      logger.error("Yikes. Master told me to do work, while I'm working.")
    // Our derivation has completed its task
    case WorkComplete(result) =>
      logger.debug("Work is complete. Result {}.", result.toString)
      master ! WorkIsDone(self)
      master ! WorkerRequestsWork(self)
      // We're idle now
      context.become(idle)
  }

  // In this state we have no work to do. There really are only
  // two messages that make sense while we're in this state, and
  // we deal with them specially here
  def idle: Receive = {
    // Master says there's work to be done, let's ask for it
    case WorkIsReady =>
      logger.debug("Requesting work")
      master ! WorkerRequestsWork(self)
    // Send the work off to the implementation
    case WorkToBeDone(work) =>
      logger.debug("Got work {}", work.toString)
      doWork(sender(), work)
      context.become(working(work))
    // We asked for it, but either someone else got it first, or
    // there's literally no work to be done
    case NoWorkToBeDone =>
  }

  override def receive = idle
}
