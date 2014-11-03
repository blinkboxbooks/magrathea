package com.blinkbox.books.marvin.magrathea.message

import java.net.URL
import java.util.UUID
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import com.blinkbox.books.config.DatabaseConfig
import com.blinkbox.books.json.DefaultFormats
import com.blinkbox.books.json.Json4sExtensions._
import com.blinkbox.books.logging.DiagnosticExecutionContext
import com.blinkbox.books.marvin.magrathea.SchemaConfig
import com.blinkbox.books.marvin.magrathea.message.Postgres._
import com.blinkbox.books.spray._
import com.github.tminglei.slickpg._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.JsonAST.{JString, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import spray.client.pipelining._
import spray.http.StatusCodes._
import spray.http.Uri.Path
import spray.http.{HttpRequest, HttpResponse, Uri}
import spray.httpx.{Json4sJacksonSupport, UnsuccessfulResponseException}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.reflectiveCalls
import scala.slick.driver.PostgresDriver
import scala.slick.jdbc.{GetResult, StaticQuery => Q}

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

object Postgres {
  object MyPostgresDriver extends PostgresDriver with PgJson4sSupport with array.PgArrayJdbcTypes {
    type DOCType = JValue
    override val jsonMethods = org.json4s.jackson.JsonMethods
    override lazy val Implicit = new Implicits with JsonImplicits
    override val simple = new Implicits with SimpleQL with JsonImplicits {
      implicit val strListTypeMapper = new SimpleArrayListJdbcType[String]("text")
    }
  }

  import com.blinkbox.books.marvin.magrathea.message.Postgres.MyPostgresDriver.simple._

  case class History(id: UUID, doc: JValue, key: String)
  case class Latest(id: UUID, doc: JValue)

  class HistoryTable(tag: Tag) extends Table[History](tag, "history") {
    def id = column[UUID]("id", O.PrimaryKey)
    def doc = column[JValue]("doc")
    def key = column[String]("key")
    def * = (id, doc, key) <> (History.tupled, History.unapply)
  }

  class LatestTable(tag: Tag) extends Table[Latest](tag, "latest") {
    def id = column[UUID]("id", O.PrimaryKey)
    def doc = column[JValue]("doc")
    def * = (id, doc) <> (Latest.tupled, Latest.unapply)
  }
}

class PostgresDocumentDao(config: DatabaseConfig, schemas: SchemaConfig) extends DocumentDao
  with Json4sJacksonSupport with JsonMethods with StrictLogging {

  import com.blinkbox.books.marvin.magrathea.message.Postgres.MyPostgresDriver.simple._

  override implicit val json4sJacksonFormats = DefaultFormats
  private implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))
  private val db = Database.forURL(config.jdbcUrl, user = config.user, password = config.pass)
  db.createConnection().close()

  val HistoryRepo = TableQuery[HistoryTable]
  val LatestRepo = TableQuery[LatestTable]

  override def getLatestDocumentById(id: String, schema: Option[String]): Future[Option[JValue]] = Future {
    db.withSession { implicit session =>
      val uuid = UUID.fromString(id)
      val doc = LatestRepo.filter(_.id === uuid).map(_.doc).firstOption
      (schema, doc) match {
        case (Some(s), Some(d)) if d \ "$schema" == JString(s) => doc
        case (None, _) => doc
        case _ => None
      }
    }
  }

  override def getHistoryDocumentById(id: String, schema: Option[String]): Future[Option[JValue]] = Future {
    db.withSession { implicit session =>
      val uuid = UUID.fromString(id)
      val doc = HistoryRepo.filter(_.id === uuid).map(_.doc).firstOption
      (schema, doc) match {
        case (Some(s), Some(d)) if d \ "$schema" == JString(s) => doc
        case (None, _) => doc
        case _ => None
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

  private def addIdRev(id: UUID): JValue = "value" -> ("_id" -> id.toString) ~ ("_rev" -> id.toString)
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
      LatestRepo
        .filter(_.doc.+>>("$schema") === schema)
        .filter(_.doc.+>>("classification") === compact(classification))
        .map(_.id)
        .list
        .map(addIdRev)
    }
  }

  override def lookupHistoryDocument(key: String): Future[List[JValue]] = Future {
    db.withSession { implicit session =>
      val jsonKey = parse(key).sha1
      HistoryRepo
        .filter(_.key === jsonKey)
        .map(_.id)
        .list
        .map(addIdRev)
    }
  }

  override def fetchHistoryDocuments(schema: String, key: String): Future[List[JValue]] = Future {
    db.withSession { implicit session =>
      val classification = key
      if (schema == schemas.book) {
        // books
        HistoryRepo
          .filter(_.doc.+>>("$schema") === schema)
          .filter(_.doc.+>>("classification") === classification)
          .map(_.doc).list
      } else {
        // contributors
        implicit val getJValueResult = GetResult(r => parse(r.nextString()))
        val contributorDocs = HistoryRepo
          .filter(_.doc.+>>("$schema") === schema)
          .filter(_.doc.+>>("classification") === classification)
          .map(_.doc).list
        val contributorFromBooks = Q.queryNA[(JValue, JValue)](
          s"""
            |select
            |  contributors.value as contributor,
            |  json_extract_path(history.doc, 'source') as source
            |from
            |  history,
            |  json_array_elements(json_extract_path(history.doc, 'contributors')) as contributors
            |where
            |  doc->>'$$schema' = '${schemas.book}' and
            |  contributors.value->>'classification' = '$classification';
          """.stripMargin).list.map {
            case (contributor, source) => contribufy(contributor, source, schemas.contributor)
          }
        contributorDocs ++ contributorFromBooks
      }
    }
  }

  override def storeLatestDocument(document: JValue): Future[String] = Future {
    db.withSession { implicit session =>
      document \ "_id" match {
        case JString(id) =>
          val docId = UUID.fromString(id)
          val latest = Latest(docId, document.removeDirectField("_id").removeDirectField("_rev"))
          LatestRepo.filter(_.id === docId).update(latest)
          id
        case _ =>
          val docId = UUID.randomUUID()
          val latest = Latest(docId, document.removeDirectField("_id").removeDirectField("_rev"))
          LatestRepo.returning(LatestRepo.map(_.id)).insert(latest).toString
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
        case JString(id) =>
          val docId = UUID.fromString(id)
          val history = History(docId, document.removeDirectField("_id").removeDirectField("_rev"), key.sha1)
          HistoryRepo.filter(_.id === docId).update(history)
        case _ =>
          val docId = UUID.randomUUID()
          val history = History(docId, document.removeDirectField("_id").removeDirectField("_rev"), key.sha1)
          HistoryRepo.insert(history)
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

class CouchDocumentDao(couchDbUrl: URL, schemas: SchemaConfig)(implicit system: ActorSystem)
  extends DocumentDao with Json4sJacksonSupport with JsonMethods with StrictLogging {

  override implicit val json4sJacksonFormats = DefaultFormats
  private implicit val ec = DiagnosticExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))
  private val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
  private val historyUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/history"))
  private val latestUri = couchDbUrl.withPath(couchDbUrl.path ++ Path("/latest"))
  private val lookupHistoryUri = historyUri.withPath(historyUri.path ++ Path("/_design/index/_view/replace_lookup"))
  private val lookupLatestUri = latestUri.withPath(latestUri.path ++ Path("/_design/index/_view/replace_lookup"))
  private val deleteHistoryUri = historyUri.withPath(historyUri.path ++ Path("/_bulk_docs"))
  private val deleteLatestUri = latestUri.withPath(latestUri.path ++ Path("/_bulk_docs"))
  private val bookUri = historyUri.withPath(historyUri.path ++ Path("/_design/history/_view/book"))
  private val contributorUri = historyUri.withPath(historyUri.path ++ Path("/_design/history/_view/contributor"))

  override def getLatestDocumentById(id: String, schema: Option[String] = None): Future[Option[JValue]] =
    getDocumentById(id, schema, latestUri)

  override def getHistoryDocumentById(id: String, schema: Option[String] = None): Future[Option[JValue]] =
    getDocumentById(id, schema, historyUri)

  override def getLatestDocumentCount(): Future[Int] = getTableDocumentCount(latestUri)

  override def getHistoryDocumentCount(): Future[Int] = getTableDocumentCount(historyUri)

  override def getAllLatestDocuments(count: Int, offset: Int): Future[List[JValue]] =
    getAllDocsFromDatabase(latestUri, count, offset)

  override def getAllHistoryDocuments(count: Int, offset: Int): Future[List[JValue]] =
    getAllDocsFromDatabase(historyUri, count, offset)

  override def lookupLatestDocument(key: String): Future[List[JValue]] = lookupDocument(key, lookupLatestUri)

  override def lookupHistoryDocument(key: String): Future[List[JValue]] = lookupDocument(key, lookupHistoryUri)

  override def fetchHistoryDocuments(schema: String, key: String): Future[List[JValue]] =
    getHistoryFetchUri(schema, key).flatMap { uri =>
      pipeline(Get(uri)).map {
        case resp if resp.status == OK => (parse(resp.entity.asString) \ "rows").children.map(_ \ "value")
        case resp => throw new UnsuccessfulResponseException(resp)
      }
    }

  override def storeLatestDocument(document: JValue): Future[String] =
    storeDocument(document, latestUri) { data =>
      val docId = (parse(data) \ "id").extract[String]
      logger.info("Stored merged document with id: {}", docId)
      docId
    }

  override def storeHistoryDocument(document: JValue): Future[Unit] =
    storeDocument(document, historyUri) { data =>
      logger.debug("Stored history document")
    }

  override def deleteLatestDocuments(documents: List[(String, String)]): Future[Unit] =
    deleteDocuments(documents, deleteLatestUri) {
      logger.debug("Deleted latest documents: {}", documents)
    }

  override def deleteHistoryDocuments(documents: List[(String, String)]): Future[Unit] =
    deleteDocuments(documents, deleteHistoryUri) {
      logger.debug("Deleted history documents: {}", documents)
    }

  private def getTableDocumentCount(uri: Uri): Future[Int] =
    pipeline(Get(uri)).map {
      case resp if resp.status == OK => (parse(resp.entity.asString) \ "doc_count").extract[Int]
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def getHistoryFetchUri(schema: String, key: String): Future[Uri] = Future {
    if (schema == schemas.book) bookUri.withQuery(("key", key))
    else if (schema == schemas.contributor) contributorUri.withQuery(("key", key))
    else throw new IllegalArgumentException(s"Unsupported schema: $schema")
  }

  private def getDocumentById(id: String, schema: Option[String] = None, uri: Uri): Future[Option[JValue]] =
    pipeline(Get(uri.withPath(uri.path ++ Path(s"/$id")))).map {
      case resp if resp.status == OK =>
        val doc = parse(resp.entity.asString).removeDirectField("_id").removeDirectField("_rev")
        schema match {
          case Some(s) if doc \ "$schema" == JString(s) => Some(doc)
          case None => Some(doc)
          case _ => None
        }
      case resp if resp.status == NotFound => None
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def getAllDocsFromDatabase(uri: Uri, count: Int, offset: Int): Future[List[JValue]] =
    pipeline(Get(uri.withPath(uri.path ++ Path("/_all_docs"))
      .withQuery(("limit", count.toString), ("skip", offset.toString), ("include_docs", "true")))).map {
      case resp if resp.status == OK => (parse(resp.entity.asString) \ "rows").children.map(_ \ "doc")
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def lookupDocument(key: String, uri: Uri): Future[List[JValue]] =
    pipeline(Get(uri.withQuery(("key", key)))).map {
      case resp if resp.status == OK => (parse(resp.entity.asString) \ "rows").children
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def storeDocument[T](document: JValue, uri: Uri)(f: => String => T): Future[T] =
    pipeline(Post(uri, document)).map {
      case resp if resp.status == Created => f(resp.entity.asString)
      case resp => throw new UnsuccessfulResponseException(resp)
    }

  private def getDeleteJson(documents: List[(String, String)]): Future[JValue] = Future {
    "docs" -> documents.map { case (id, rev) =>
      ("_id" -> id) ~ ("_rev" -> rev) ~ ("_deleted" -> true)
    }
  }

  private def deleteDocuments(documents: List[(String, String)], uri: Uri)(f: => Unit): Future[Unit] =
    getDeleteJson(documents).flatMap { json =>
      pipeline(Post(uri, json)).map {
        case resp if resp.status == Created => f
        case resp => throw new UnsuccessfulResponseException(resp)
      }
    }
}
