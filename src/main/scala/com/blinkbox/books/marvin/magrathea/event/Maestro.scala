package com.blinkbox.books.marvin.magrathea.event

import akka.actor.{Actor, ActorLogging}

class Maestro extends Actor with ActorLogging {
  override def receive = {
    case msg => log.info("--> received " + msg.toString)
  }
}
