package com.blinkbox.books.marvin.magrathea.message

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.MessageListenerConfig
import com.blinkbox.books.messaging.ActorErrorHandler
import com.blinkbox.books.rabbitmq.RabbitMqConfirmedPublisher.PublisherConfiguration
import com.blinkbox.books.rabbitmq.RabbitMqConsumer.QueueConfiguration
import com.blinkbox.books.rabbitmq.{RabbitMq, RabbitMqConfirmedPublisher, RabbitMqConsumer}

class MessageListener(config: MessageListenerConfig) {
  implicit val system = ActorSystem("magrathea-message")
  implicit val executionContext = DiagnosticExecutionContext(system.dispatcher)
  implicit val timeout = Timeout(config.actorTimeout)
  sys.addShutdownHook(system.shutdown())

  val publisherConnection = newConnection()
  val consumerConnection = newConnection()

  val messageDao = new DefaultMessageDao(config.couchDbUrl, config.schema)

  val messageErrorHandler = errorHandler("message-error", config.error)
  val messageHandler = system.actorOf(Props(new MessageHandler(messageDao,
    messageErrorHandler, config.retryInterval)), name = "message-handler")
  val messageConsumer = consumer("message-consumer", config.input, messageHandler)

  def start() {
    messageConsumer ! RabbitMqConsumer.Init
  }

  private def newConnection() = RabbitMq.reliableConnection(config.rabbitMq)

  private def errorHandler(actorName: String, config: PublisherConfiguration) =
    new ActorErrorHandler(publisher(actorName, config))

  private def consumer(actorName: String, config: QueueConfiguration, handler: ActorRef) =
    system.actorOf(Props(new RabbitMqConsumer(consumerConnection.createChannel, config, s"$actorName-msg", handler)), actorName)

  private def publisher(actorName: String, config: PublisherConfiguration) =
    system.actorOf(Props(new RabbitMqConfirmedPublisher(publisherConnection, config)), actorName)
}
