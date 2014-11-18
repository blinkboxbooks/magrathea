package com.blinkbox.books.marvin.magrathea.message

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.blinkbox.books.marvin.magrathea.AppConfig
import com.blinkbox.books.marvin.magrathea.api.IndexService
import com.blinkbox.books.messaging.ActorErrorHandler
import com.blinkbox.books.rabbitmq.RabbitMqConfirmedPublisher.PublisherConfiguration
import com.blinkbox.books.rabbitmq.RabbitMqConsumer.QueueConfiguration
import com.blinkbox.books.rabbitmq.{RabbitMq, RabbitMqConfirmedPublisher, RabbitMqConsumer}

import scala.concurrent.ExecutionContext

class MessageListener(config: AppConfig, indexService: IndexService, documentDao: DocumentDao)
  (implicit system: ActorSystem, ex: ExecutionContext, timeout: Timeout) {
  val consumerConnection = RabbitMq.reliableConnection(config.listener.rabbitmq)
  val publisherConnection = RabbitMq.recoveredConnection(config.listener.rabbitmq)

  val distributor = new DocumentDistributor(config.listener.distributor, config.schemas)

  val messageErrorHandler = errorHandler("message-error", config.listener.error)
  val messageHandler = system.actorOf(Props(new MessageHandler(config.schemas, documentDao,
    distributor, indexService, messageErrorHandler, config.listener.retryInterval)
    (DocumentMerger.merge)), name = "message-handler")
  val messageConsumer = consumer("message-consumer", config.listener.input, messageHandler)

  def start() {
    messageConsumer ! RabbitMqConsumer.Init
  }

  private def errorHandler(actorName: String, config: PublisherConfiguration) =
    new ActorErrorHandler(publisher(actorName, config))

  private def consumer(actorName: String, config: QueueConfiguration, handler: ActorRef) =
    system.actorOf(Props(new RabbitMqConsumer(
      consumerConnection.createChannel, config, s"$actorName-msg", handler)), actorName)

  private def publisher(actorName: String, config: PublisherConfiguration) =
    system.actorOf(Props(new RabbitMqConfirmedPublisher(publisherConnection, config)), actorName)
}
