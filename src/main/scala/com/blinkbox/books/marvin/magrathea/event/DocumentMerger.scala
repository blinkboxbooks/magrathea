package com.blinkbox.books.marvin.magrathea.event

import org.json4s.JsonAST.JValue

object DocumentMerger {
  case class DifferentClassificationException(msg: String = null) extends RuntimeException(msg)

  def merge(a: JValue, b: JValue): JValue = {
    a merge b
  }
}
