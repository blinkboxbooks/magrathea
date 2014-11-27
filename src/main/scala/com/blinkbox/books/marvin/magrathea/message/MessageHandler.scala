package com.blinkbox.books.marvin.magrathea.message

import java.util.UUID

import akka.actor.ActorRef
import akka.util.Timeout
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.marvin.magrathea.api.IndexService
import com.blinkbox.books.marvin.magrathea.{History, SchemaConfig}
import com.blinkbox.books.messaging.{ErrorHandler, Event, ReliableEventHandler}
import com.typesafe.scalalogging.StrictLogging
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
  indexService: IndexService, errorHandler: ErrorHandler, retryInterval: FiniteDuration)
  (documentMerge: => (JValue, JValue) => JValue) extends ReliableEventHandler(errorHandler, retryInterval)
  with StrictLogging with Json4sJacksonSupport with JsonMethods {

  implicit val timeout = Timeout(retryInterval)
  implicit val json4sJacksonFormats = DefaultFormats

  override protected def handleEvent(event: Event, originalSender: ActorRef): Future[Unit] = timeTaken(for {
    incomingDoc <- parseDocument(event.body.asString())
    document = normaliseContributors(incomingDoc)
    _ <- handleDocument(document)
    _ <- handleContributors(document)
  } yield ())

  // Consider the error temporary if the exception or its root cause is an IO exception, timeout or connection exception.
  @tailrec
  final override protected def isTemporaryFailure(e: Throwable): Boolean =
    e.isInstanceOf[TimeoutException] || e.isInstanceOf[ConnectionException] ||
    Option(e.getCause).isDefined && isTemporaryFailure(e.getCause)

  private def timeTaken[T](block: => Future[T]): Future[T] = for {
    t0 <- Future.successful(System.currentTimeMillis())
    res <- block
    t1 = System.currentTimeMillis()
    _ = logger.info(s"Elapsed time to handle document: ${t1 - t0}ms")
  } yield res

  private def parseDocument(json: String): Future[JValue] = Future {
    logger.info("Received document")
    parse(json)
  }

  private def normaliseContributors(doc: JValue): JValue = {
    (doc \ "$schema", doc \ "contributors") match {
      case (JString(schema), JArray(arr)) if schema == schemas.book =>
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

  private def handleDocument(document: JValue, deleteOld: Boolean = true): Future[Unit] = for {
    (insertId, deletedIds) <- documentDao.storeHistoryDocument(document, deleteOld)
    history <- documentDao.getDocumentHistory(document)
    mergedDoc = mergeHistoryDocuments(history)
    (insertId, deletedIds) <- documentDao.storeCurrentDocument(mergedDoc, deleteOld)
    _ <- distributor.sendDistributionInformation(mergedDoc) zip indexify(mergedDoc, insertId, deletedIds)
  } yield ()

  private def handleContributors(document: JValue): Future[Unit] = document \ "contributors" match {
    case JArray(arr) =>
      logger.info("Merging contributors")
      val source = document \ "source"
      Future.sequence(arr.map { contributor =>
        handleDocument(contribufy(contributor, source), deleteOld = false)
      }).map(_ => ())
    case _ => Future.successful(())
  }

  private def indexify(document: JValue, insertId: UUID, deletedIds: List[UUID]): Future[Unit] = {
    val indexed = indexService.indexCurrentDocument(insertId, document).map(_ => ())
    if (deletedIds.nonEmpty) (indexService.deleteCurrentIndex(deletedIds) zip indexed).map(_ => ()) else indexed
  }

  private def contribufy(document: JValue, source: JValue): JValue = {
    val schemaField: JValue = "$schema" -> schemas.contributor
    val sourceField: JValue = "source" -> source
    schemaField merge document merge sourceField
  }

  private def mergeHistoryDocuments(documents: List[History]): JValue = {
    logger.debug("Starting history document merging...")
    val merged = documents.map(_.toJson) match {
      case Nil => throw new IllegalArgumentException("Expected to merge a non-empty history list")
      case x :: Nil => DocumentAnnotator.annotate(x)
      case x => x.par.reduce(documentMerge)
    }
    logger.info("Merged document")
    merged
  }
}
