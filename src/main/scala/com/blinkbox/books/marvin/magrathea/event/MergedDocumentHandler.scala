package com.blinkbox.books.marvin.magrathea.event

import akka.actor.Actor
import com.typesafe.scalalogging.slf4j.StrictLogging

class MergedDocumentHandler extends Actor with StrictLogging {
  implicit val ec = context.dispatcher

  override def receive: Receive = {
    case msg => println("Got " + msg)
  }
}
