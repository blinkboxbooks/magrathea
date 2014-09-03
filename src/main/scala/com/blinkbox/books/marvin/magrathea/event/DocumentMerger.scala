package com.blinkbox.books.marvin.magrathea.event

import org.json4s.JsonAST.JValue

object DocumentMerger {
  def merge(a: JValue, b: JValue): JValue = {
    a merge b
  }
}
