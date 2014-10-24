package com.blinkbox.books.marvin.magrathea.message

import akka.actor.ActorRef
import akka.util.Timeout
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.marvin.magrathea.SchemaConfig
import com.blinkbox.books.marvin.magrathea.api.SearchService
import com.blinkbox.books.messaging.{ErrorHandler, Event, ReliableEventHandler}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import spray.can.Http.ConnectionException
import spray.httpx.Json4sJacksonSupport

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, TimeoutException}
import scala.language.{implicitConversions, postfixOps}

class MessageHandler(schemas: SchemaConfig, documentDao: DocumentDao, distributor: DocumentDistributor,
  searchService: SearchService, errorHandler: ErrorHandler, retryInterval: FiniteDuration)
  (documentMerge: (JValue, JValue) => JValue) extends ReliableEventHandler(errorHandler, retryInterval)
  with StrictLogging with Json4sJacksonSupport with JsonMethods {

  implicit val timeout = Timeout(retryInterval)
  implicit val json4sJacksonFormats = DefaultFormats

  override protected def handleEvent(event: Event, originalSender: ActorRef): Future[Unit] = for {
    incomingDoc <- parseDocument(event.body.asString())
    historyLookupKey = extractHistoryLookupKey(incomingDoc)
    historyLookupKeyMatches <- documentDao.lookupHistoryDocument(historyLookupKey)
    normalisedIncomingDoc = normaliseHistoryDocument(incomingDoc, historyLookupKeyMatches)
    _ <- normaliseDatabase(historyLookupKeyMatches)(documentDao.deleteHistoryDocuments)
    _ <- documentDao.storeHistoryDocument(normalisedIncomingDoc)
    (schema, classification) = extractSchemaAndClassification(normalisedIncomingDoc)
    _ <- mergeStoreDistribute(schema, classification) zip contributorMerge(normalisedIncomingDoc)
  } yield ()

  // Consider the error temporary if the exception or its root cause is an IO exception, timeout or connection exception.
  @tailrec
  final override protected def isTemporaryFailure(e: Throwable): Boolean =
    e.isInstanceOf[TimeoutException] || e.isInstanceOf[ConnectionException] ||
    Option(e.getCause).isDefined && isTemporaryFailure(e.getCause)

  private def contributorMerge(document: JValue): Future[Unit] = document \ "contributors" match {
    case JArray(arr) =>
      logger.info("Merging contributors")
      Future.sequence(arr.map { contributor =>
        mergeStoreDistribute(schemas.contributor, extractClassification(contributor))
      }).map(_ => ())
    case _ => Future.successful(())
  }

  private def mergeStoreDistribute(schema: String, classification: String): Future[Unit] = for {
    history <- documentDao.fetchHistoryDocuments(schema, classification)
    mergedDoc = mergeDocuments(history)
    latestLookupKey = extractLatestLookupKey(mergedDoc)
    latestLookupKeyMatches <- documentDao.lookupLatestDocument(latestLookupKey)
    normalisedMergedDoc = normaliseDocument(mergedDoc, latestLookupKeyMatches)
    _ <- normaliseDatabase(latestLookupKeyMatches)(documentDao.deleteLatestDocuments)
    docId <- documentDao.storeLatestDocument(normalisedMergedDoc)
    _ <- distributor.sendDistributionInformation(normalisedMergedDoc) zip indexify(normalisedMergedDoc, docId)
  } yield ()

  private def indexify(document: JValue, docId: String): Future[Unit] =
    Future(DocumentAnnotator.deAnnotate(document)).flatMap { deAnnotated =>
      searchService.indexDocument(deAnnotated.removeDirectField("_id").removeDirectField("_rev"), docId)
    }.map(_ => ())

  private def mergeDocuments(documents: List[JValue]): JValue = {
    logger.debug("Starting document merging...")
    val merged = documents match {
      case Nil => throw new IllegalArgumentException("Expected to merge a non-empty history list")
      case x :: Nil => DocumentAnnotator.annotate(x)
      case x => x.par.reduce(documentMerge)
    }
    logger.info("Merged document")
    merged.removeDirectField("_id").removeDirectField("_rev")
  }

  private def parseDocument(json: String): Future[JValue] = Future {
    logger.info("Received document")
    parse(json)
  }

  private def normaliseHistoryDocument(document: JValue, lookupKeyMatches: List[JValue]): JValue = {
    val doc = normaliseDocument(document, lookupKeyMatches)
    doc \ "contributors" match {
      case JArray(arr) =>
        val newArr: JValue = arr.map { contributor =>
          val name = contributor \ "names" \ "display"
          if (name == JNothing) throw new IllegalArgumentException(
            s"Cannot extract display name from contributor: ${compact(render(contributor))}")
          val oldIds: JValue = "ids" -> contributor \ "ids"
          val newIds: JValue = "ids" -> ("bbb" -> name.sha1)
          val ids = newIds merge oldIds
          val classification: JValue = "classification" -> (contributor \ "classification").toOption
            .getOrElse[JValue](List(("realm" -> "bbb_id") ~ ("id" -> name.sha1)))
          classification merge contributor merge ids
        }
        doc.overwriteDirectField("contributors", newArr)
      case _ => doc
    }
  }

  private def normaliseDocument(document: JValue, lookupKeyMatches: List[JValue]): JValue =
    // Merging the value of the first row with the rest of the document. This will result in a document with
    // "_id" and "_rev" fields, which means replace the previous document with this new one.
    // If the first row does not exist, the document will remain unchanged.
    lookupKeyMatches.headOption match {
      case Some(item) =>
        logger.debug("Normalised document to override")
        val (id, rev) = extractIdRev(item)
        document merge (("_id" -> id) ~ ("_rev" -> rev))
      case None => document
    }

  private def normaliseDatabase(lookupKeyMatches: List[JValue])(f: List[(String, String)] => Future[Unit]): Future[Unit] =
    // If there is at least on document with that key, delete all these documents except the first
    if (lookupKeyMatches.size > 1) f(lookupKeyMatches.drop(1).map(extractIdRev)) else Future.successful(())

  private def extractIdRev(item: JValue): (String, String) = {
    val id = item \ "value" \ "_id"
    val rev = item \ "value" \ "_rev"
    if (id == JNothing || rev == JNothing)
      throw new IllegalArgumentException(s"Cannot extract _id and _rev: ${compact(render(item))}")
    (id.extract[String], rev.extract[String])
  }

  private def extractHistoryLookupKey(document: JValue): String = {
    val schema = document \ "$schema"
    val source = (document \ "source")
      .removeDirectField("processedAt")
      .remove(_ == document \ "source" \ "system" \ "version")
    val classification = document \ "classification"
    if (schema == JNothing || source == JNothing || classification == JNothing)
      throw new IllegalArgumentException(
        s"Cannot extract history lookup key (schema, source, classification): ${compact(render(document))}")
    val key = compact(render(List(schema, source, classification)))
    logger.debug("Extracted history lookup key: {}", key)
    key
  }

  private def extractLatestLookupKey(document: JValue): String = {
    val schema = document \ "$schema"
    val classification = document \ "classification"
    if (schema == JNothing || classification == JNothing)
      throw new IllegalArgumentException(
        s"Cannot extract latest lookup key (schema, classification): ${compact(render(document))}")
    val key = compact(render(("$schema" -> schema) ~ ("classification" -> classification)))
    logger.debug("Extracted latest lookup key: {}", key)
    key
  }

  private def extractClassification(document: JValue): String = {
    val classification = document \ "classification"
    if (classification == JNothing)
      throw new IllegalArgumentException(s"Cannot extract classification: ${compact(render(document))}")
    compact(render(classification))
  }

  private def extractSchemaAndClassification(document: JValue): (String, String) = {
    val schema = document \ "$schema"
    if (schema == JNothing)
      throw new IllegalArgumentException(s"Cannot extract schema: ${compact(render(document))}")
    (schema.extract[String], extractClassification(document))
  }
}
