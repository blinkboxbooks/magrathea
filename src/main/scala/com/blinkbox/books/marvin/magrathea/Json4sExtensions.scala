package com.blinkbox.books.marvin.magrathea

import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods

import scala.language.implicitConversions

// TODO: Consider moving this to a common place?
object Json4sExtensions {
  private val md = java.security.MessageDigest.getInstance("SHA-1")
  class BetterJValue(val v: JValue) extends JsonMethods {
    def removeDirectField(key: String): JValue = v.remove(_ == v \ key)
    def replaceDirectField(key: String, value: JValue) = {
      val newKey: JValue = key -> value
      removeDirectField(key).merge(newKey)
    }
    lazy val sha1 = md.digest(compact(v).getBytes("UTF-8")).map("%02x".format(_)).mkString
  }
  implicit def jValueToBetterJValue(v: JValue) = new BetterJValue(v)
}
