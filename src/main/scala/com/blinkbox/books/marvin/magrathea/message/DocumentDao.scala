package com.blinkbox.books.marvin.magrathea.message

import java.util.UUID
import java.util.concurrent.Executors

import com.blinkbox.books.config.DatabaseConfig
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.SchemaConfig
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.JsonAST.{JString, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.{ExecutionContext, Future}
import scala.language.reflectiveCalls
import scala.slick.jdbc.{GetResult, SetParameter, StaticQuery => Q}
import scala.util.Try

case class History(id: UUID, schema: String, doc: JValue, source: JValue, classification: JValue)
case class Latest(id: UUID, doc: JValue)

trait DocumentDao {
  def getLatestDocumentById(id: String, schema: Option[String] = None): Future[Option[JValue]]
  def getHistoryDocumentById(id: String, schema: Option[String] = None): Future[Option[JValue]]
  def getLatestDocumentCount(): Future[Int]
  def getHistoryDocumentCount(): Future[Int]
  def getAllLatestDocuments(count: Int, offset: Int): Future[List[JValue]]
  def getAllHistoryDocuments(count: Int, offset: Int): Future[List[JValue]]
  def lookupLatestDocument(key: String): Future[List[JValue]]
  def lookupHistoryDocument(key: String): Future[List[JValue]]
  def fetchHistoryDocuments(schema: String, key: String): Future[List[JValue]]
  def storeLatestDocument(document: JValue): Future[String]
  def storeHistoryDocument(document: JValue): Future[Unit]
  def deleteLatestDocuments(documents: List[(String, String)]): Future[Unit]
  def deleteHistoryDocuments(documents: List[(String, String)]): Future[Unit]
}

class PostgresDocumentDao(config: DatabaseConfig, schemas: SchemaConfig) extends DocumentDao
  with Json4sJacksonSupport with JsonMethods with StrictLogging {
  import scala.slick.driver.PostgresDriver.simple._

  class HistoryTable(tag: Tag) extends Table[History](tag, "history") {
    def id = column[UUID]("id", O.PrimaryKey)
    def schema = column[String]("schema")
    def doc = column[JValue]("doc")
    def source = column[JValue]("source")
    def classification = column[JValue]("classification")
    def * = (id, schema, doc, source, classification) <> (History.tupled, History.unapply)
  }

  class LatestTable(tag: Tag) extends Table[Latest](tag, "latest") {
    def id = column[UUID]("id", O.PrimaryKey)
    def doc = column[JValue]("doc")
    def * = (id, doc) <> (Latest.tupled, Latest.unapply)
  }

  override implicit val json4sJacksonFormats = DefaultFormats
  private implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))
  private val db = Database.forURL(config.jdbcUrl, user = config.user, password = config.pass)
  db.createConnection().close()

  private val HistoryRepo = TableQuery[HistoryTable]
  private val LatestRepo = TableQuery[LatestTable]
  private implicit val getJValueResult = GetResult(r => parse(r.nextString()))

  private val lookupLatestDocumentQuery = Q.query[(String, JValue), String](
    """
      |SELECT
      |  id
      |FROM
      |  latest
      |WHERE
      |  doc->>'$schema' = ? AND
      |  doc->'classification' @> ?'
    """.stripMargin)

  private val lookupHistoryDocumentQuery = Q.query[String, String](
    """
      |SELECT id FROM history WHERE
      |  key = ?
    """.stripMargin)

  private val fetchHistoryDocumentQuery = Q.query[(String, String), JValue](
    """
      |SELECT
      |  doc
      |FROM
      |  history
      |WHERE
      |  doc->>'$schema' = ? AND
      |  doc->'classification' @> ?
    """.stripMargin)

  private val fetchHistoryContributorsFromBooksQuery = Q.query[String, (JValue, JValue)](
    s"""
      |SELECT
      |  contributors.value,
      |  jsonb_extract_path(history.doc, 'source') AS source
      |FROM
      |  history,
      |  jsonb_array_elements(jsonb_extract_path(history.doc, 'contributors')) AS contributors
      |WHERE
      |  doc->>'$$schema' = '${schemas.book}' AND
      |  contributors.value->'classification' @> ?;
    """.stripMargin)

  private val updateHistoryDocumentQuery = Q.update[(JValue, String, String)](
    """
      |UPDATE
      |  history
      |SET
      |  doc = ?,
      |  key = ?
      |WHERE
      |  id = ?
    """.stripMargin)

  private val insertHistoryDocumentQuery = Q.update[(JValue, String)](
    """
      |INSERT INTO history (doc, key) VALUES(?, ?)
    """.stripMargin)

  private val updateLatestDocumentQuery = Q.update[(JValue, String)](
    s"""
       |UPDATE
       |  latest
       |SET
       |  doc = ?
       |WHERE
       |  id = ?
    """.stripMargin)

  private val insertLatestDocumentQuery = new Q[String, String](
    """
      |INSERT INTO latest (doc) VALUES(?) RETURNING id
    """.stripMargin, SetParameter.SetString, GetResult.GetString)

  override def getLatestDocumentById(id: String, schema: Option[String]): Future[Option[JValue]] = Future {
    db.withSession { implicit session =>
      Try(UUID.fromString(id)).toOption.flatMap { uuid =>
        val doc = LatestRepo.filter(_.id === uuid).map(_.doc).firstOption
        (schema, doc) match {
          case (Some(s), Some(d)) if d \ "$schema" == JString(s) => doc
          case (None, _) => doc
          case _ => None
        }
      }
    }
  }

  override def getHistoryDocumentById(id: String, schema: Option[String]): Future[Option[JValue]] = Future {
    db.withSession { implicit session =>
      Try(UUID.fromString(id)).toOption.flatMap { uuid =>
        val doc = HistoryRepo.filter(_.id === uuid).map(_.doc).firstOption
        (schema, doc) match {
          case (Some(s), Some(d)) if d \ "$schema" == JString(s) => doc
          case (None, _) => doc
          case _ => None
        }
      }
    }
  }

  override def getLatestDocumentCount(): Future[Int] = Future {
    db.withSession { implicit session =>
      LatestRepo.size.run
    }
  }

  override def getHistoryDocumentCount(): Future[Int] = Future {
    db.withSession { implicit session =>
      HistoryRepo.size.run
    }
  }

  override def getAllLatestDocuments(count: Int, offset: Int): Future[List[JValue]] = Future {
    db.withSession { implicit session =>
      LatestRepo.drop(offset).take(count).map(_.doc).list
    }
  }

  override def getAllHistoryDocuments(count: Int, offset: Int): Future[List[JValue]] = Future {
    db.withSession { implicit session =>
      HistoryRepo.drop(offset).take(count).map(_.doc).list
    }
  }

  private def escape(s: String): String = s.replace("'", "''")
  private def addIdRev(id: String): JValue = "value" -> ("_id" -> id) ~ ("_rev" -> id)
  private def contribufy(doc: JValue, source: JValue, schema: String): JValue = {
    val schemaField: JValue = "$schema" -> schema
    val sourceField: JValue = "source" -> source
    schemaField merge doc.removeDirectField("role") merge sourceField
  }

  override def lookupLatestDocument(key: String): Future[List[JValue]] = Future {
    db.withSession { implicit session =>
      val json = parse(key)
      val schema = json.children(0).extract[String]
      val classification = json.children(1)
      lookupLatestDocumentQuery(schema, classification).list.map(addIdRev)
    }
  }

  override def lookupHistoryDocument(key: String): Future[List[JValue]] = Future {
    db.withSession { implicit session =>
      val jsonKey = parse(key).sha1
      lookupHistoryDocumentQuery(jsonKey).list.map(addIdRev)
    }
  }

  override def fetchHistoryDocuments(schema: String, key: String): Future[List[JValue]] = Future {
    db.withSession { implicit session =>
      val classification = escape(key)
      val contributorDocs = fetchHistoryDocumentQuery(schema, classification).list
      if (schema == schemas.contributor) {
        val contributorFromBooks = fetchHistoryContributorsFromBooksQuery(classification).list.map {
          case (contributor, source) => contribufy(contributor, source, schemas.contributor)
        }
        contributorDocs ++ contributorFromBooks
      } else contributorDocs
    }
  }

  override def storeLatestDocument(document: JValue): Future[String] = Future {
    db.withSession { implicit session =>
      document \ "_id" match {
        case JString(id) =>
          updateLatestDocumentQuery(document, id).execute
          id
        case _ => insertLatestDocumentQuery(compact(document)).first
      }
    }
  }

  override def storeHistoryDocument(document: JValue): Future[Unit] = Future {
    db.withSession { implicit session =>
      val schema = document \ "$schema"
      val source = (document \ "source")
        .removeDirectField("processedAt")
        .remove(_ == document \ "source" \ "system" \ "version")
      val classification = document \ "classification"
      val key = render(List(schema, source, classification))
      document \ "_id" match {
        case JString(id) => updateHistoryDocumentQuery(document, key.sha1, id).execute
        case _ => insertHistoryDocumentQuery(document, key.sha1).execute
      }
    }
  }

  override def deleteLatestDocuments(documents: List[(String, String)]): Future[Unit] = Future {
    db.withSession { implicit session =>
      val ids = documents.map(x => UUID.fromString(x._1))
      LatestRepo.filter(_.id inSet ids).delete
    }
  }

  override def deleteHistoryDocuments(documents: List[(String, String)]): Future[Unit] = Future {
    db.withSession { implicit session =>
      val ids = documents.map(x => UUID.fromString(x._1))
      HistoryRepo.filter(_.id inSet ids).delete
    }
  }
}
