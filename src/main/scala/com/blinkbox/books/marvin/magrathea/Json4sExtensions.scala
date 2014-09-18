package com.blinkbox.books.marvin.magrathea

import org.json4s._

import scala.language.implicitConversions

// TODO: Consider moving this to a common place?
object Json4sExtensions {
  class BetterJValue(val v: JValue) {
    def removeDirectField(field: String): JValue = v.remove(_ == v \ field)
  }
  implicit def jValueToBetterJValue(v: JValue) = new BetterJValue(v)
}
