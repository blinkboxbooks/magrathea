package com.blinkbox.books.marvin.magrathea

import java.net.URL

import com.blinkbox.books.marvin.magrathea.Realm.Realm
import com.blinkbox.books.marvin.magrathea.Role.Role
import com.blinkbox.books.marvin.magrathea.UriType.UriType
import org.joda.time.DateTime

case class AuthorImage(classification: Classification, media: ImageMedia, source: Source)

case class ImageMedia(images: List[MediaItem])
case class MediaItem(classification: List[Classification], uris: List[UriItem], size: Int, source: Option[Source])
case class UriItem(`type`: UriType, uri: URL, params: Option[String])

case class Classification(realm: Realm, id: String)
case class Source(`$remaining`: Remaining)
case class Remaining(deliveredAt: DateTime, role: Role, username: String, uri: Option[URL], system: Option[System], processedAt: Option[DateTime])
case class System(name: String, version: String)

object Realm extends Enumeration {
  type Realm = Value
  val Publisher = Value("publisher")
  val Isbn = Value("isbn")
  val ContributorId = Value("contributor_id")
  val Type = Value("type")
  val SourceKey = Value("source_key")
  val Format = Value("format")
}

object Role extends Enumeration {
  type Role = Value
  val PublisherFtp = Value("publisher_ftp")
  val ContentManager = Value("content_manager")
}

object UriType extends Enumeration {
  type UriType = Value
  val Static = Value("stgatic")
  val ResourceServer = Value("resource_server")
}
