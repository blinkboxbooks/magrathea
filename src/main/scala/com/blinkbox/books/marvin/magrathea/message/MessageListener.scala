package com.blinkbox.books.marvin.magrathea.message

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.ListenerConfig
import com.blinkbox.books.messaging.ActorErrorHandler
import com.blinkbox.books.rabbitmq.RabbitMqConfirmedPublisher.PublisherConfiguration
import com.blinkbox.books.rabbitmq.RabbitMqConsumer.QueueConfiguration
import com.blinkbox.books.rabbitmq.{RabbitMq, RabbitMqConfirmedPublisher, RabbitMqConsumer}

class MessageListener(config: ListenerConfig) {
  implicit val system = ActorSystem("magrathea-message")
  implicit val executionContext = DiagnosticExecutionContext(system.dispatcher)
  implicit val timeout = Timeout(config.actorTimeout)
  sys.addShutdownHook(system.shutdown())

  val consumerConnection = RabbitMq.reliableConnection(config.rabbitMq)
  val publisherConnection = RabbitMq.recoveredConnection(config.rabbitMq)

  val documentDao = new DefaultDocumentDao(config.couchDbUrl, config.schemas)
  val distributor = new DocumentDistributor(config.distributor, config.schemas)

  val messageErrorHandler = errorHandler("message-error", config.error)
  val messageHandler = system.actorOf(Props(new MessageHandler(config.schemas, documentDao, distributor,
    messageErrorHandler, config.retryInterval)(DocumentMerger.merge)), name = "message-handler")
  val messageConsumer = consumer("message-consumer", config.input, messageHandler)

  def start() {
    messageConsumer ! RabbitMqConsumer.Init
  }

  private def errorHandler(actorName: String, config: PublisherConfiguration) =
    new ActorErrorHandler(publisher(actorName, config))

  private def consumer(actorName: String, config: QueueConfiguration, handler: ActorRef) =
    system.actorOf(Props(new RabbitMqConsumer(consumerConnection.createChannel, config, s"$actorName-msg", handler)), actorName)

  private def publisher(actorName: String, config: PublisherConfiguration) =
    system.actorOf(Props(new RabbitMqConfirmedPublisher(publisherConnection, config)), actorName)
}
