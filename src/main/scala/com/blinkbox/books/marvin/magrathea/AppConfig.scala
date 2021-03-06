package com.blinkbox.books.marvin.magrathea

import java.net.URL
import java.util.concurrent.TimeUnit

import com.blinkbox.books.config._
import com.blinkbox.books.rabbitmq.RabbitMqConfig
import com.blinkbox.books.rabbitmq.RabbitMqConfirmedPublisher.PublisherConfiguration
import com.blinkbox.books.rabbitmq.RabbitMqConsumer.QueueConfiguration
import com.typesafe.config.Config

import scala.concurrent.duration._

case class AppConfig(api: ApiConfig, listener: ListenerConfig,
  elasticsearch: ElasticConfig, schemas: SchemaConfig, db: DatabaseConfig)
case class ListenerConfig(rabbitmq: RabbitMqConfig, retryInterval: FiniteDuration, actorTimeout: FiniteDuration,
  distributor: DistributorConfig, input: QueueConfiguration, error: PublisherConfiguration)
case class SchemaConfig(book: String, contributor: String)
case class DistributorConfig(output: PublisherConfiguration)
case class ElasticConfig(url: URL, index: String, reIndexChunks: Int) {
  val host = url.getHost
  val port = url.getPort
}

object AppConfig {
  val prefix = "service.magrathea"
  def apply(config: Config) = new AppConfig(
    ApiConfig(config, s"$prefix.api.public"),
    ListenerConfig(config, s"$prefix.messageListener"),
    ElasticConfig(config, s"$prefix.elasticsearch"),
    SchemaConfig(config, s"$prefix.schema"),
    DatabaseConfig(config, s"$prefix.db")
  )
}

object ListenerConfig {
  def apply(config: Config, prefix: String) = new ListenerConfig(
    RabbitMqConfig(config.getConfig(s"${AppConfig.prefix}")),
    config.getDuration(s"$prefix.retryInterval", TimeUnit.SECONDS).seconds,
    config.getDuration(s"$prefix.actorTimeout", TimeUnit.SECONDS).seconds,
    DistributorConfig(config, s"$prefix.distributor"),
    QueueConfiguration(config.getConfig(s"$prefix.input")),
    PublisherConfiguration(config.getConfig(s"$prefix.error"))
  )
}

object DistributorConfig {
  def apply(config: Config, prefix: String) = new DistributorConfig(
    PublisherConfiguration(config.getConfig(s"$prefix.output"))
  )
}

object SchemaConfig {
  def apply(config: Config, prefix: String) = new SchemaConfig(
    config.getString(s"$prefix.book"),
    config.getString(s"$prefix.contributor")
  )
}

object ElasticConfig {
  def apply(config: Config, prefix: String) = new ElasticConfig(
    config.getUrl(s"$prefix.url"),
    config.getString(s"$prefix.index"),
    config.getInt(s"$prefix.reIndexChunks")
  )
}
