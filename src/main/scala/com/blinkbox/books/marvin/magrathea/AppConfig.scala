package com.blinkbox.books.marvin.magrathea

import java.net.URL
import java.util.concurrent.TimeUnit

import com.blinkbox.books.config._
import com.blinkbox.books.rabbitmq.RabbitMqConfig
import com.blinkbox.books.rabbitmq.RabbitMqConfirmedPublisher.PublisherConfiguration
import com.blinkbox.books.rabbitmq.RabbitMqConsumer.QueueConfiguration
import com.typesafe.config.Config

import scala.concurrent.duration._

case class AppConfig(service: ServiceConfig, eventListener: EventListenerConfig, swagger: SwaggerConfig)
case class ServiceConfig(api: ApiConfig, myKey: Int)
case class EventListenerConfig(rabbitMq: RabbitMqConfig, couchDbUrl: URL, retryInterval: FiniteDuration,
  actorTimeout: FiniteDuration, schema: SchemaConfig, input: QueueConfiguration, error: PublisherConfiguration)
case class SchemaConfig(book: String, contributor: String)

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
    config.getHttpUrl(s"$prefix.couchdb.url"),
    config.getDuration(s"$prefix.retryInterval", TimeUnit.SECONDS).seconds,
    config.getDuration(s"$prefix.actorTimeout", TimeUnit.SECONDS).seconds,
    SchemaConfig(config, s"$prefix.schema"),
    QueueConfiguration(config.getConfig(s"$prefix.input")),
    PublisherConfiguration(config.getConfig(s"$prefix.error"))
  )
}

object SchemaConfig {
  def apply(config: Config, prefix: String) = new SchemaConfig(
    config.getString(s"$prefix.book"),
    config.getString(s"$prefix.contributor")
  )
}
