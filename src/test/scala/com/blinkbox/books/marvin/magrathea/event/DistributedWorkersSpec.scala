/**
 * Taken from:
 * http://letitcrash.com/post/29044669086/balancing-workload-across-nodes-with-akka-2
 */
package com.blinkbox.books.marvin.magrathea.event

import akka.actor.{ActorPath, ActorRef, ActorSystem, Props}
import akka.pattern.PipeToSupport
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest._

import scala.concurrent.Future

class DistributedWorkersSpec extends TestKit(ActorSystem("WorkerSpec"))
  with Matchers with WordSpecLike with BeforeAndAfterAll with ImplicitSender {

  class TestWorker(masterLocation: ActorPath) extends Worker(masterLocation) with PipeToSupport {
    // We'll use the current dispatcher for the execution context.
    // You can use whatever you want.
    implicit val ec = context.dispatcher

    override def doWork(workSender: ActorRef, msg: Any): Unit = {
      Future {
        workSender ! msg
        WorkComplete("done")
      } pipeTo self
    }
  }

  override def afterAll() {
    system.shutdown()
  }

  def worker(name: String) = system.actorOf(Props(
    new TestWorker(ActorPath.fromString(
      "akka://%s/user/%s".format(system.name, name)))))

  "Worker" should {
    "work" in {
      // Spin up the master
      val m = system.actorOf(Props[Master], "master")
      // Create three workers
      val w1 = worker("master")
      val w2 = worker("master")
      val w3 = worker("master")
      // Send some work to the master
      m ! "Hithere"
      m ! "Guys"
      m ! "So"
      m ! "What's"
      m ! "Up?"
      // We should get it all back
      expectMsgAllOf("Hithere", "Guys", "So", "What's", "Up?")
    }
  }
}
