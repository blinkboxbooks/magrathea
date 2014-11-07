package com.blinkbox.books.marvin.magrathea.message

import java.util.concurrent.Executors

import com.blinkbox.books.config.DatabaseConfig
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.SchemaConfig
import com.github.tminglei.slickpg.PgJson4sSupport
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.JsonAST.{JNothing, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.{ExecutionContext, Future}
import scala.language.reflectiveCalls
import scala.slick.driver.PostgresDriver
import scala.slick.jdbc.{GetResult, SetParameter, StaticQuery => Q}

case class History(id: String, schema: String, classification: JValue, doc: JValue, source: JValue) {
  lazy val toJson: JValue = {
    val schemaField: JValue = "$schema" -> schema
    val classificationField: JValue = "classification" -> classification
    val sourceField: JValue = "source" -> source
    schemaField merge classificationField merge doc merge sourceField
  }
}
case class Latest(id: String, schema: String, classification: JValue, doc: JValue, source: JValue) {
  lazy val toJson: JValue = {
    val schemaField: JValue = "$schema" -> schema
    val classificationField: JValue = "classification" -> classification
    val sourceField: JValue = "source" -> source
    schemaField merge classificationField merge doc merge sourceField
  }
}

trait DocumentDao {
  def getHistoryDocumentById(id: String, schema: Option[String] = None): Future[Option[JValue]]
  def getLatestDocumentById(id: String, schema: Option[String] = None): Future[Option[JValue]]
  def countHistoryDocuments(): Future[Int]
  def countLatestDocuments(): Future[Int]
  def getHistoryDocuments(count: Int, offset: Int): Future[List[JValue]]
  def getLatestDocuments(count: Int, offset: Int): Future[List[JValue]]
  def storeHistoryDocument(document: JValue, deleteOld: Boolean = true): Future[(String, List[String])]
  def storeLatestDocument(document: JValue, deleteOld: Boolean = true): Future[(String, List[String])]
  def getDocumentHistory(document: JValue): Future[List[JValue]]
}

object MyPostgresDriver extends PostgresDriver with PgJson4sSupport {
  override type DOCType = JValue
  override val jsonMethods = JsonMethods
  override lazy val Implicit = new Implicits with JsonImplicits
  override val simple = new Implicits with SimpleQL with JsonImplicits
}

class PostgresDocumentDao(config: DatabaseConfig, schemas: SchemaConfig) extends DocumentDao
  with Json4sJacksonSupport with JsonMethods with StrictLogging {
  import com.blinkbox.books.marvin.magrathea.message.MyPostgresDriver.simple._

  private implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))
  override implicit val json4sJacksonFormats = DefaultFormats

  class HistoryTable(tag: Tag) extends Table[History](tag, "history") {
    def id = column[String]("id", O.PrimaryKey)
    def schema = column[String]("schema")
    def classification = column[JValue]("classification")
    def doc = column[JValue]("doc")
    def source = column[JValue]("source")
    def * = (id, schema, classification, doc, source) <> (History.tupled, History.unapply)
  }

  class LatestTable(tag: Tag) extends Table[Latest](tag, "latest") {
    def id = column[String]("id", O.PrimaryKey)
    def schema = column[String]("schema")
    def classification = column[JValue]("classification")
    def doc = column[JValue]("doc")
    def source = column[JValue]("source")
    def * = (id, schema, classification, doc, source) <> (Latest.tupled, Latest.unapply)
  }

  private val db = Database.forURL(config.jdbcUrl, user = config.user, password = config.pass)
  private val HistoryRepo = TableQuery[HistoryTable]
  private val LatestRepo = TableQuery[LatestTable]

  private implicit val GetHistoryResult = GetResult[History](r => History(
    r.nextString(), r.nextString(), parse(r.nextString()), parse(r.nextString()), parse(r.nextString())))
  private implicit val SetJValueParameter = SetParameter[JValue]((v, pp) => pp.setString(compact(render(v))))

  private val deleteHistoryDocumentsContainingSource = Q.query[JValue, String](
    "DELETE FROM history WHERE source @> ?::jsonb RETURNING id")

  private val deleteLatestDocumentsContainingSource = Q.query[JValue, String](
    "DELETE FROM latest WHERE source @> ?::jsonb RETURNING id")

  private val insertHistoryDocument = Q.query[(String, JValue, JValue, JValue), String](
    "INSERT INTO history (schema, classification, doc, source) VALUES(?, ?::jsonb, ?::jsonb, ?::jsonb) RETURNING id")

  private val insertLatestDocument = Q.query[(String, JValue, JValue, JValue), String](
    "INSERT INTO latest (schema, classification, doc, source) VALUES(?, ?::jsonb, ?::jsonb, ?::jsonb) RETURNING id")

  private val selectDocumentHistory = Q.query[(String, JValue), History]("" +
    "SELECT id, schema, classification, doc, source FROM history WHERE schema = ? AND classification = ?::jsonb")

  override def getHistoryDocumentById(id: String, schema: Option[String]): Future[Option[JValue]] = Future {
    db.withSession { implicit session =>
      val doc = HistoryRepo.withFilter(_.id === id).firstOption
      (schema, doc) match {
        case (Some(s), Some(d)) if d.schema == s => Option(d.toJson)
        case (None, Some(d)) => Option(d.toJson)
        case _ => None
      }
    }
  }

  override def getLatestDocumentById(id: String, schema: Option[String]): Future[Option[JValue]] = Future {
    db.withSession { implicit session =>
      val doc = LatestRepo.withFilter(_.id === id).firstOption
      (schema, doc) match {
        case (Some(s), Some(d)) if d.schema == s => Option(d.toJson)
        case (None, Some(d)) => Option(d.toJson)
        case _ => None
      }
    }
  }

  override def countHistoryDocuments(): Future[Int] = Future {
    db.withSession { implicit session => HistoryRepo.size.run }
  }

  override def countLatestDocuments(): Future[Int] = Future {
    db.withSession { implicit session => LatestRepo.size.run }
  }

  override def getHistoryDocuments(count: Int, offset: Int): Future[List[JValue]] = Future {
    db.withSession { implicit session => HistoryRepo.drop(offset).take(count).map(_.doc).list }
  }

  override def getLatestDocuments(count: Int, offset: Int): Future[List[JValue]] = Future {
    db.withSession { implicit session => LatestRepo.drop(offset).take(count).map(_.doc).list }
  }

  override def storeHistoryDocument(document: JValue, deleteOld: Boolean): Future[(String, List[String])] =
    storeDocument(document, deleteOld)(insertHistoryDocument, deleteHistoryDocumentsContainingSource)

  override def storeLatestDocument(document: JValue, deleteOld: Boolean): Future[(String, List[String])] =
    storeDocument(document, deleteOld)(insertLatestDocument, deleteLatestDocumentsContainingSource)

  override def getDocumentHistory(document: JValue): Future[List[JValue]] = Future {
    db.withSession { implicit session =>
      val schema = document \ "$schema"
      val classification = document \ "classification"
      if (schema == JNothing || classification == JNothing) throw new IllegalArgumentException(
        s"Cannot find document schema, classification: ${compact(render(document))}")
      selectDocumentHistory(schema.extract[String], classification).list.map(_.toJson)
    }
  }

  private def storeDocument(document: JValue, deleteOld: Boolean)(insert: Q[(String, JValue, JValue, JValue), String],
    deleteContainingSource: Q[JValue, String]): Future[(String, List[String])] = Future {
    db.withTransaction { implicit session =>
      val schema = document \ "$schema"
      val classification = document \ "classification"
      val source = document \ "source"
      if (schema == JNothing || classification == JNothing || source == JNothing) throw new IllegalArgumentException(
        s"Cannot find document schema, classification and source: ${compact(render(document))}")
      val doc = document.removeDirectField("$schema").removeDirectField("classification").removeDirectField("source")
      val deleted = if (deleteOld) {
        val keySource = source.removeDirectField("processedAt").remove(_ == source \ "system" \ "version")
        deleteContainingSource(keySource).list
      } else List.empty
      val inserted = insert(schema.extract[String], classification, doc, source).first
      (inserted, deleted)
    }
  }
}
