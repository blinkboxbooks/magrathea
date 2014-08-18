package com.blinkbox.books.marvin.magrathea

import java.util.concurrent.TimeUnit

import com.blinkbox.books.config._
import com.blinkbox.books.rabbitmq.RabbitMqConfig
import com.blinkbox.books.rabbitmq.RabbitMqConfirmedPublisher.PublisherConfiguration
import com.blinkbox.books.rabbitmq.RabbitMqConsumer.QueueConfiguration
import com.typesafe.config.Config

import scala.concurrent.duration._

case class AppConfig(service: ServiceConfig, eventListener: EventListenerConfig, swagger: SwaggerConfig)
case class ServiceConfig(api: ApiConfig, myKey: Int)
case class EventListenerConfig(rabbitMq: RabbitMqConfig, retryInterval: FiniteDuration, actorTimeout: FiniteDuration,
                               book: ComponentConfig, contributor: ComponentConfig)
case class ComponentConfig(input: QueueConfiguration, error: PublisherConfiguration)

object AppConfig {
  val prefix = "service.magrathea"
  def apply(config: Config) = new AppConfig(
    ServiceConfig(config, s"$prefix.api.public"),
    EventListenerConfig(config, s"$prefix.eventListener"),
    SwaggerConfig(config, 1)
  )
}

object ServiceConfig {
  def apply(config: Config, prefix: String) = new ServiceConfig(
    ApiConfig(config, prefix),
    config.getInt(s"$prefix.myKey")
  )
}

object EventListenerConfig {
  def apply(config: Config, prefix: String) = new EventListenerConfig(
    RabbitMqConfig(config),
    config.getDuration(s"$prefix.retryInterval", TimeUnit.SECONDS).seconds,
    config.getDuration(s"$prefix.actorTimeout", TimeUnit.SECONDS).seconds,
    ComponentConfig(config, s"$prefix.book"),
    ComponentConfig(config, s"$prefix.contributor")
  )
}

object ComponentConfig {
  def apply(config: Config, prefix: String) = new ComponentConfig(
    QueueConfiguration(config.getConfig(s"$prefix.input")),
    PublisherConfiguration(config.getConfig(s"$prefix.error"))
  )
}
