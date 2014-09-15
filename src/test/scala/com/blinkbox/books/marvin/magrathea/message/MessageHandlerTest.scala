package com.blinkbox.books.marvin.magrathea.message

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.blinkbox.books.test.MockitoSyrup
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FlatSpecLike}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MessageHandlerTest extends TestKit(ActorSystem("test-system"))
  with ImplicitSender with FlatSpecLike with BeforeAndAfter with MockitoSyrup {

  trait TestFixture {
    val messageDao = mock[DocumentDao]
  }
}
