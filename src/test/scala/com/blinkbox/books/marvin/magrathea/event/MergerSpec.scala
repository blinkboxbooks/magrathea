package com.blinkbox.books.marvin.magrathea.event

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.blinkbox.books.marvin.magrathea.MergerConfig
import com.blinkbox.books.marvin.magrathea.event.Merger.{MergeRequest, MergeResponse}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

class MergerSpec extends TestKit(ActorSystem("MergerSpec"))
  with FunSuiteLike with BeforeAndAfterAll with ImplicitSender {

  private def merge(a: Int, b: Int) = a + b

  override def afterAll(): Unit = system.shutdown()

  test("Sending 1 should return 1") {
    val merger = system.actorOf(Props(new Merger(MergerConfig(2, 1), self)(merge)))
    merger ! MergeRequest(List(1))
    expectMsg(MergeResponse(1))
  }

  test("Merging 2 numbers with 3 maxDocsPerJob and 4 workers") {
    val merger = system.actorOf(Props(new Merger(MergerConfig(3, 4), self)(merge)))
    merger ! MergeRequest(List(1, 2))
    expectMsg(MergeResponse(3))
  }

  test("Merging 3 numbers with 3 maxDocsPerJob and 4 workers") {
    val merger = system.actorOf(Props(new Merger(MergerConfig(3, 4), self)(merge)))
    merger ! MergeRequest(List(1, 2, 3))
    expectMsg(MergeResponse(6))
  }

  test("Merging 4 numbers with 3 maxDocsPerJob and 4 workers") {
    val merger = system.actorOf(Props(new Merger(MergerConfig(3, 4), self)(merge)))
    merger ! MergeRequest(List(1, 2, 3, 4))
    expectMsg(MergeResponse(10))
  }

  test("Merging 4 numbers with 2 maxDocsPerJob and 3 workers") {
    val merger = system.actorOf(Props(new Merger(MergerConfig(2, 3), self)(merge)))
    merger ! MergeRequest(List(1, 2, 3, 4))
    expectMsg(MergeResponse(10))
  }

  test("Merging 4 numbers with 2 maxDocsPerJob and 2 workers") {
    val merger = system.actorOf(Props(new Merger(MergerConfig(2, 2), self)(merge)))
    merger ! MergeRequest(List(1, 2, 3, 4))
    expectMsg(MergeResponse(10))
  }

  test("Merging 4 numbers with 3 maxDocsPerJob and 2 workers") {
    val merger = system.actorOf(Props(new Merger(MergerConfig(3, 2), self)(merge)))
    merger ! MergeRequest(List(1, 2, 3, 4))
    expectMsg(MergeResponse(10))
  }

  test("Merging 5 numbers with 2 maxDocsPerJob and 2 workers") {
    val merger = system.actorOf(Props(new Merger(MergerConfig(2, 2), self)(merge)))
    merger ! MergeRequest(List(1, 2, 3, 4, 5))
    expectMsg(MergeResponse(15))
  }

  test("Merging 10 numbers with 3 maxDocsPerJob and 4 workers") {
    val merger = system.actorOf(Props(new Merger(MergerConfig(3, 4), self)(merge)))
    merger ! MergeRequest(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    expectMsg(MergeResponse(55))
  }

  test("Merging 100 numbers with odd maxDocsPerJob") {
    val merger = system.actorOf(Props(new Merger(MergerConfig(3, 4), self)(merge)))
    val list = (1 to 10).toList
    merger ! MergeRequest(list)
    expectMsg(MergeResponse(list.sum))
  }

  test("Merging 100 numbers with even maxDocsPerJob") {
    val merger = system.actorOf(Props(new Merger(MergerConfig(4, 4), self)(merge)))
    val list = (1 to 10).toList
    merger ! MergeRequest(list)
    expectMsg(MergeResponse(list.sum))
  }

  test("Merging 100 numbers with 1 worker") {
    val merger = system.actorOf(Props(new Merger(MergerConfig(2, 1), self)(merge)))
    val list = (1 to 10).toList
    merger ! MergeRequest(list)
    expectMsg(MergeResponse(list.sum))
  }
}
