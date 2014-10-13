package com.blinkbox.books.marvin.magrathea

import java.net.URL
import java.util.concurrent.TimeUnit

import com.blinkbox.books.config._
import com.blinkbox.books.rabbitmq.RabbitMqConfig
import com.blinkbox.books.rabbitmq.RabbitMqConfirmedPublisher.PublisherConfiguration
import com.blinkbox.books.rabbitmq.RabbitMqConsumer.QueueConfiguration
import com.typesafe.config.Config

import scala.concurrent.duration._

case class AppConfig(service: ServiceConfig, listener: ListenerConfig, couchDbUrl: URL, schemas: SchemaConfig)
case class ServiceConfig(api: ApiConfig)
case class ListenerConfig(rabbitMq: RabbitMqConfig, retryInterval: FiniteDuration, actorTimeout: FiniteDuration,
                          distributor: DistributorConfig, input: QueueConfiguration, error: PublisherConfiguration)
case class SchemaConfig(book: String, contributor: String)
case class DistributorConfig(bookOutput: PublisherConfiguration, contributorOutput: PublisherConfiguration)

object AppConfig {
  val prefix = "service.magrathea"
  def apply(config: Config) = new AppConfig(
    ServiceConfig(config, s"$prefix.api.public"),
    ListenerConfig(config, s"$prefix.messageListener"),
    config.getHttpUrl(s"$prefix.couchdb.url"),
    SchemaConfig(config, s"$prefix.schema")
  )
}

object ServiceConfig {
  def apply(config: Config, prefix: String) = new ServiceConfig(
    ApiConfig(config, prefix)
  )
}

object ListenerConfig {
  def apply(config: Config, prefix: String) = new ListenerConfig(
    RabbitMqConfig(config),
    config.getDuration(s"$prefix.retryInterval", TimeUnit.SECONDS).seconds,
    config.getDuration(s"$prefix.actorTimeout", TimeUnit.SECONDS).seconds,
    DistributorConfig(config, s"$prefix.distributor"),
    QueueConfiguration(config.getConfig(s"$prefix.input")),
    PublisherConfiguration(config.getConfig(s"$prefix.error"))
  )
}

object DistributorConfig {
  def apply(config: Config, prefix: String) = new DistributorConfig(
    PublisherConfiguration(config.getConfig(s"$prefix.book.output")),
    PublisherConfiguration(config.getConfig(s"$prefix.contributor.output"))
  )
}

object SchemaConfig {
  def apply(config: Config, prefix: String) = new SchemaConfig(
    config.getString(s"$prefix.book"),
    config.getString(s"$prefix.contributor")
  )
}
