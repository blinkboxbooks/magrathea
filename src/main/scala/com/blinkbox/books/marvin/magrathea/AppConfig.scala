package com.blinkbox.books.marvin.magrathea

import com.blinkbox.books.config._
import com.blinkbox.books.rabbitmq.RabbitMqConsumer.QueueConfiguration
import com.typesafe.config.Config

case class AppConfig(service: ServiceConfig, eventListener: EventListenerConfig, swagger: SwaggerConfig)
case class ServiceConfig(api: ApiConfig, myKey: Int)
case class EventListenerConfig(ePubVerifier: QueueConfiguration, coverProcessor: QueueConfiguration)

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
    QueueConfiguration(config.getConfig(s"$prefix.ePubVerifier.input")),
    QueueConfiguration(config.getConfig(s"$prefix.coverProcessor.input"))
  )
}
