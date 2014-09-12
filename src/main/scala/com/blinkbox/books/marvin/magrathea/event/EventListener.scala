package com.blinkbox.books.marvin.magrathea.event

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.EventListenerConfig
import com.blinkbox.books.messaging.ActorErrorHandler
import com.blinkbox.books.rabbitmq.RabbitMqConfirmedPublisher.PublisherConfiguration
import com.blinkbox.books.rabbitmq.RabbitMqConsumer.QueueConfiguration
import com.blinkbox.books.rabbitmq.{RabbitMq, RabbitMqConfirmedPublisher, RabbitMqConsumer}

class EventListener(config: EventListenerConfig) {
  implicit val system = ActorSystem("magrathea-event")
  implicit val executionContext = DiagnosticExecutionContext(system.dispatcher)
  implicit val timeout = Timeout(config.actorTimeout)
  sys.addShutdownHook(system.shutdown())

  val publisherConnection = newConnection()
  val consumerConnection = newConnection()

  val eventErrorHandler = errorHandler("event-error", config.error)
  val eventHandler = system.actorOf(Props(new MessageHandler(config.couchdbUrl,
    config.schema, eventErrorHandler, config.retryInterval)), name = "event-handler")
  val eventConsumer = consumer("event-consumer", config.input, eventHandler)

  def start() {
    eventConsumer ! RabbitMqConsumer.Init
  }

  private def newConnection() = RabbitMq.reliableConnection(config.rabbitMq)

  private def errorHandler(actorName: String, config: PublisherConfiguration) =
    new ActorErrorHandler(publisher(actorName, config))

  private def consumer(actorName: String, config: QueueConfiguration, handler: ActorRef) =
    system.actorOf(Props(new RabbitMqConsumer(consumerConnection.createChannel, config, s"$actorName-msg", handler)), actorName)

  private def publisher(actorName: String, config: PublisherConfiguration) =
    system.actorOf(Props(new RabbitMqConfirmedPublisher(publisherConnection, config)), actorName)
}
