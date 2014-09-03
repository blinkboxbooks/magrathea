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
case class EventListenerConfig(rabbitMq: RabbitMqConfig, couchdbUrl: URL, retryInterval: FiniteDuration,
  actorTimeout: FiniteDuration, merger: MergerConfig, book: ComponentConfig, contributor: ComponentConfig)
case class ComponentConfig(schema: String, input: QueueConfiguration, error: PublisherConfiguration)
case class MergerConfig(maxItemsPerJob: Int, numOfWorkers: Int)

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
    MergerConfig(config, s"$prefix.merger"),
    ComponentConfig(config, s"$prefix.book"),
    ComponentConfig(config, s"$prefix.contributor")
  )
}

object ComponentConfig {
  def apply(config: Config, prefix: String) = new ComponentConfig(
    config.getString(s"$prefix.schema"),
    QueueConfiguration(config.getConfig(s"$prefix.input")),
    PublisherConfiguration(config.getConfig(s"$prefix.error"))
  )
}

object MergerConfig {
  def apply(config: Config, prefix: String) = new MergerConfig(
    config.getInt(s"$prefix.maxItemsPerJob"),
    config.getInt(s"$prefix.numOfWorkers")
  )
}
