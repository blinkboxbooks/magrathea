package com.blinkbox.books.marvin.magrathea.event

import akka.actor.Actor
import com.blinkbox.books.marvin.magrathea.event.Merger.MergeResult
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods

class MergedDocumentHandler extends Actor with StrictLogging with JsonMethods {
  implicit val ec = context.dispatcher

  override def receive: Receive = {
    case msg: MergeResult[JValue] =>
      println(pretty(render(msg.item)))
    case msg => println("Got " + msg)
  }
}
