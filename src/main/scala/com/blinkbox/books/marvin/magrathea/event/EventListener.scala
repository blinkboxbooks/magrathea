package com.blinkbox.books.marvin.magrathea.event

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.blinkbox.books.marvin.magrathea.EventListenerConfig
import com.blinkbox.books.messaging.ActorErrorHandler
import com.blinkbox.books.rabbitmq.RabbitMqConfirmedPublisher.PublisherConfiguration
import com.blinkbox.books.rabbitmq.RabbitMqConsumer.QueueConfiguration
import com.blinkbox.books.rabbitmq.{RabbitMq, RabbitMqConfirmedPublisher, RabbitMqConsumer}

class EventListener(config: EventListenerConfig) {
  implicit val system = ActorSystem("magrathea-event")
  implicit val executionContext = system.dispatcher
  implicit val timeout = Timeout(config.actorTimeout)
  sys.addShutdownHook(system.shutdown())

  val publisherConnection = newConnection()
  val consumerConnection = newConnection()

  val maestro = system.actorOf(Props[Maestro], "maestro")

  // cover processor
  val coverProcessorErrorHandler = errorHandler("cover-processor-error", config.coverProcessor.error)
  val coverProcessorMsgHandler = system.actorOf(Props(new CoverProcessorMessageHandler(
    maestro, coverProcessorErrorHandler, config.retryInterval)), name = "cover-processor-handler")
  val coverProcessorConsumer = consumer("cover-processor-consumer", config.coverProcessor.input, coverProcessorMsgHandler)

  // author image
  val authorImageErrorHandler = errorHandler("author-image-error", config.authorImage.error)
  val authorImageMsgHandler = system.actorOf(Props(new AuthorImageMessageHandler(
    maestro, authorImageErrorHandler, config.retryInterval)), name = "author-image-handler")
  val authorImageConsumer = consumer("author-image-consumer", config.authorImage.input, authorImageMsgHandler)

  // epub verifier
  val ePubVerifierErrorHandler = errorHandler("epub-verifier-error", config.ePubVerifier.error)
  val ePubVerifierMsgHandler = system.actorOf(Props(new ePubVerifierMessageHandler(
    maestro, ePubVerifierErrorHandler, config.retryInterval)), name = "epub-verifier-handler")
  val ePubVerifierConsumer = consumer("epub-verifier-consumer", config.ePubVerifier.input, ePubVerifierMsgHandler)

  // epub encrypter
  val ePubEncrypterErrorHandler = errorHandler("epub-encrypter-error", config.ePubEncrypter.error)
  val ePubEncrypterMsgHandler = system.actorOf(Props(new ePubEncrypterMessageHandler(
    maestro, ePubEncrypterErrorHandler, config.retryInterval)), name = "epub-encrypter-handler")
  val ePubEncrypterConsumer = consumer("epub-encrypter-consumer", config.ePubEncrypter.input, ePubEncrypterMsgHandler)

  def start() {
    coverProcessorConsumer ! RabbitMqConsumer.Init
    authorImageConsumer ! RabbitMqConsumer.Init
    ePubVerifierConsumer ! RabbitMqConsumer.Init
    ePubEncrypterConsumer ! RabbitMqConsumer.Init
  }

  private def newConnection() = RabbitMq.reliableConnection(config.rabbitMq)

  private def errorHandler(actorName: String, config: PublisherConfiguration) =
    new ActorErrorHandler(publisher(actorName, config))

  private def consumer(actorName: String, config: QueueConfiguration, handler: ActorRef) =
    system.actorOf(Props(new RabbitMqConsumer(consumerConnection.createChannel, config, s"$actorName-msg", handler)), actorName)

  private def publisher(actorName: String, config: PublisherConfiguration) =
    system.actorOf(Props(new RabbitMqConfirmedPublisher(publisherConnection.createChannel, config)), actorName)
}
