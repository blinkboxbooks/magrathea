package com.blinkbox.books.marvin.magrathea.event

import org.json4s._
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import shapeless.Typeable._

object DocumentMerger {
  case class DifferentClassificationException(msg: String = null) extends RuntimeException(msg)

  val StaticKeys = Seq("source", "classification", "$schema")
  val AuthorityIndex = Seq("publisher_ftp", "content_manager")

  def merge(docA: JValue, docB: JValue): JValue = {
    // "b" will be emitted, so update it with data from "a". If an element of "a"
    // is newer, ensure there's an object representing the times things were updated.
    var res = docB
    val a: Map[String, Any] = docA.asInstanceOf[JObject].values
    val b: Map[String, Any] = docA.asInstanceOf[JObject].values
    a.withFilter(noStaticKeys).foreach { case(key, value) =>
      val aIsBetterThanB = for {
        aSource <- (docA)
        bSource <- (b.get("source") orElse b.get("source")).map(_.cast[Map[String, Any]])
      } yield true
      val aIsAuthorisedToReplaceB = for {
        a <- 1
      } yield true

      var replaceContents = true

      res = docB merge docA
    }
    res
  }

  // We don't compare these fields, as they're not source data or are important to the comparison
  private val noStaticKeys: ((String, Any)) => Boolean = {
    case (key, _) => !StaticKeys.contains(key)
  }
}
