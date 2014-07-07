package com.blinkbox.books.marvin.magrathea

import com.blinkbox.books.config._
import com.typesafe.config.Config

case class AppConfig(service: ServiceConfig, swagger: SwaggerConfig)
case class ServiceConfig(api: ApiConfig, myKey: Int)

object AppConfig {
  val prefix = "service.magrathea.api.public"
  def apply(config: Config) = new AppConfig(
    ServiceConfig(config, prefix),
    SwaggerConfig(config, 1)
  )
}

object ServiceConfig {
  def apply(config: Config, prefix: String) = new ServiceConfig(
    ApiConfig(config, prefix),
    config.getInt(s"$prefix.myKey")
  )
}
