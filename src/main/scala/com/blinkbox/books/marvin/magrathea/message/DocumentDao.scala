package com.blinkbox.books.marvin.magrathea.message

import java.util.UUID
import java.util.concurrent.ForkJoinPool

import com.blinkbox.books.config.DatabaseConfig
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.Helpers._
import com.blinkbox.books.marvin.magrathea.{Current, History, JsonDoc, SchemaConfig}
import com.github.tminglei.slickpg.PgJson4sSupport
import com.typesafe.scalalogging.StrictLogging
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods
import spray.httpx.Json4sJacksonSupport

import scala.concurrent.{ExecutionContext, Future}
import scala.language.reflectiveCalls
import scala.slick.driver.PostgresDriver
import scala.slick.jdbc.{GetResult, SetParameter, StaticQuery => Q, StaticQueryInvoker}

trait DocumentDao {
  def getHistoryDocumentById(id: UUID, schema: Option[String] = None): Future[Option[History]]
  def getCurrentDocumentById(id: UUID, schema: Option[String] = None): Future[Option[Current]]
  def countHistoryDocuments(): Future[Int]
  def countCurrentDocuments(): Future[Int]
  def getHistoryDocuments(count: Int, offset: Int): Future[List[History]]
  def getCurrentDocuments(count: Int, offset: Int): Future[List[Current]]
  def storeHistoryDocument(document: JValue, deleteOld: Boolean = true): Future[(UUID, List[UUID])]
  def storeCurrentDocument(document: JValue, deleteOld: Boolean = true): Future[(UUID, List[UUID])]
  def getDocumentHistory(document: JValue): Future[List[History]]
  def getDocumentHistory(id: UUID, schema: String): Future[List[History]]
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

  private implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(new ForkJoinPool()))
  override implicit val json4sJacksonFormats = DefaultFormats

  class HistoryTable(tag: Tag) extends Table[History](tag, "history_documents") {
    def id = column[UUID]("id", O.PrimaryKey)
    def schema = column[String]("schema")
    def classification = column[JValue]("classification")
    def doc = column[JValue]("doc")
    def source = column[JValue]("source")
    def * = (id, schema, classification, doc, source) <> (History.tupled, History.unapply)
  }

  class CurrentTable(tag: Tag) extends Table[Current](tag, "current_documents") {
    def id = column[UUID]("id", O.PrimaryKey)
    def schema = column[String]("schema")
    def classification = column[JValue]("classification")
    def doc = column[JValue]("doc")
    def source = column[JValue]("source")
    def * = (id, schema, classification, doc, source) <> (Current.tupled, Current.unapply)
  }

  private val db = Database.forURL(config.jdbcUrl, user = config.user, password = config.pass)
  private val HistoryRepo = TableQuery[HistoryTable]
  private val CurrentRepo = TableQuery[CurrentTable]

  private implicit val GetUUID = GetResult[UUID](r => UUID.fromString(r.nextString()))
  private implicit val GetHistoryResult = GetResult[History](r => History(
    UUID.fromString(r.nextString()), r.nextString(), parse(r.nextString()), parse(r.nextString()), parse(r.nextString())))
  private implicit val SetJValueParameter = SetParameter[JValue]((v, pp) => pp.setString(compact(render(v))))
  private implicit val SetUUIDParameter = SetParameter[UUID]((v, pp) => pp.setString(v.toString))

  private val deleteHistoryDocuments = Q.query[JValue, UUID](
    "DELETE FROM history_documents WHERE source @> ?::jsonb RETURNING id")

  private val deleteCurrentDocuments = Q.query[(JValue, JValue), UUID](
    "DELETE FROM current_documents WHERE source @> ?::jsonb OR classification = ?::jsonb RETURNING id")

  private val insertHistoryDocument = Q.query[(String, JValue, JValue, JValue), UUID](
    "INSERT INTO history_documents (schema, classification, doc, source) VALUES(?, ?::jsonb, ?::jsonb, ?::jsonb) RETURNING id")

  private val insertCurrentDocument = Q.query[(String, JValue, JValue, JValue), UUID](
    "INSERT INTO current_documents (schema, classification, doc, source) VALUES(?, ?::jsonb, ?::jsonb, ?::jsonb) RETURNING id")

  private val selectDocumentHistory = Q.query[(String, JValue), History](
    "SELECT id, schema, classification, doc, source FROM history_documents WHERE schema = ? AND classification = ?::jsonb")

  private val selectDocumentHistoryById = Q.query[(UUID, String), History](
    """
      |SELECT history_documents.id, history_documents.schema, history_documents.classification, history_documents.doc, history_documents.source
      |FROM current_documents
      |INNER JOIN history_documents ON
      |  current_documents.schema = history_documents.schema AND
      |  current_documents.classification = history_documents.classification
      |WHERE current_documents.id = ?::uuid AND current_documents.schema = ?
      |ORDER BY history_documents.source->'deliveredAt' ASC
    """.stripMargin)

  override def getHistoryDocumentById(id: UUID, schema: Option[String]): Future[Option[History]] = Future {
    db.withSession { implicit s => getDocumentOfSchema(schema, HistoryRepo.withFilter(_.id === id).firstOption) }
  }

  override def getCurrentDocumentById(id: UUID, schema: Option[String]): Future[Option[Current]] = Future {
    db.withSession { implicit s => getDocumentOfSchema(schema, CurrentRepo.withFilter(_.id === id).firstOption) }
  }

  override def countHistoryDocuments(): Future[Int] = Future {
    db.withSession { implicit s => HistoryRepo.size.run }
  }

  override def countCurrentDocuments(): Future[Int] = Future {
    db.withSession { implicit s => CurrentRepo.size.run }
  }

  override def getHistoryDocuments(count: Int, offset: Int): Future[List[History]] = Future {
    db.withSession { implicit s => HistoryRepo.drop(offset).take(count).list }
  }

  override def getCurrentDocuments(count: Int, offset: Int): Future[List[Current]] = Future {
    db.withSession { implicit s => CurrentRepo.drop(offset).take(count).list }
  }

  override def storeHistoryDocument(document: JValue, deleteOld: Boolean): Future[(UUID, List[UUID])] =
    storeDocument(document, deleteOld, insertHistoryDocument) { case (keySource, _) =>
      deleteHistoryDocuments(keySource)
    }

  override def storeCurrentDocument(document: JValue, deleteOld: Boolean): Future[(UUID, List[UUID])] =
    storeDocument(document, deleteOld, insertCurrentDocument) { case (keySource, classification) =>
      deleteCurrentDocuments(keySource, classification)
    }

  override def getDocumentHistory(document: JValue): Future[List[History]] = Future {
    db.withSession { implicit s =>
      extractFieldsFrom(document) match { case (schema, classification, _, _) =>
        selectDocumentHistory(schema, classification).list
      }
    }
  }

  override def getDocumentHistory(id: UUID, schema: String): Future[List[History]] = Future {
    db.withSession { implicit s => selectDocumentHistoryById(id, schema).list }
  }

  private def getDocumentOfSchema[T <: JsonDoc](schema: Option[String], doc: Option[T]): Option[T] =
    (schema, doc) match {
      case (Some(s), doc @ Some(d)) if d.schema == s => doc
      case (None, doc @ Some(d)) => doc
      case _ => None
    }

  private def storeDocument(document: JValue, deleteOld: Boolean, insert: Q[(String, JValue, JValue, JValue), UUID])
    (delete: => (JValue, JValue) => StaticQueryInvoker[_, UUID]): Future[(UUID, List[UUID])] = Future {
    db.withTransaction { implicit s =>
      extractFieldsFrom(document) match { case (schema, classification, doc, source) =>
        val deleted = if (deleteOld) delete(extractKeySource(source), classification).list else List.empty
        val inserted = insert(schema, classification, doc, source).first
        (inserted, deleted)
      }
    }
  }

  private def extractKeySource(source: JValue): JValue =
    source.removeDirectField("processedAt").remove(_ == source \ "system" \ "version")
}
