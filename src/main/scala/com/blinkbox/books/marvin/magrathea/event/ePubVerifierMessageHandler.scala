package com.blinkbox.books.marvin.magrathea.event

import java.io.IOException

import akka.actor.ActorRef
import akka.util.Timeout
import com.blinkbox.books.messaging.{ErrorHandler, Event, ReliableEventHandler}

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, TimeoutException}

class ePubVerifierMessageHandler(maestro: ActorRef, errorHandler: ErrorHandler, retryInterval: FiniteDuration)
  extends ReliableEventHandler(errorHandler, retryInterval) {

  implicit val timeout = Timeout(retryInterval)

  override protected def handleEvent(event: Event, originalSender: ActorRef) = Future {
    log.info("---- received: " + event.body.asString())
  }

  // Consider the error temporary if the exception or its root cause is an IO exception or timeout.
  @tailrec
  final override protected def isTemporaryFailure(e: Throwable) = e.isInstanceOf[IOException] ||
    e.isInstanceOf[TimeoutException] || Option(e.getCause).isDefined && isTemporaryFailure(e.getCause)
}
