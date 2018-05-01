/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.core.database.mongodb

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.ByteBuffer
import java.security.MessageDigest

import scala.concurrent.Future
import akka.actor.ActorSystem
import akka.event.Logging.ErrorLevel
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString
import com.mongodb.client.gridfs.model.GridFSUploadOptions
import org.apache.commons.io.IOUtils
import org.bson.json.{JsonMode, JsonWriterSettings}
import org.mongodb.scala.bson.{BsonMaxKey, BsonMinKey, BsonNull, BsonString}
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.gridfs.GridFSBucket
import org.mongodb.scala.model._
import org.mongodb.scala.{MongoClient, MongoException}
import spray.json._
import whisk.common.{Logging, LoggingMarkers, TransactionId}
import whisk.core.database._
import whisk.core.entity._
import whisk.core.database.StoreUtils._
import whisk.http.Messages

case class MongoFile(_id: String, filename: String, metadata: Map[String, String])
object MongoFile extends DefaultJsonProtocol {
  implicit val serds = jsonFormat3(MongoFile.apply)
}

/**
 * Basic client to put and delete artifacts in a data store.
 *
 * @param client the mongodb client to access database
 * @param dbName the name of the database to operate on
 * @param collection the name of the collection to operate on
 * @param useBatching whether use batch update/insert
 */
class MongoDbStore[DocumentAbstraction <: DocumentSerializer](client: MongoClient,
                                                              dbName: String,
                                                              collection: String,
                                                              useBatching: Boolean = false)(
  implicit system: ActorSystem,
  val logging: Logging,
  jsonFormat: RootJsonFormat[DocumentAbstraction],
  materializer: ActorMaterializer,
  docReader: DocumentReader)
    extends ArtifactStore[DocumentAbstraction]
    with DefaultJsonProtocol {

  protected[core] implicit val executionContext = system.dispatcher

  private val database = client.getDatabase(dbName)
  private val main_collection = database.getCollection(collection)
  private val gridFSBucket = GridFSBucket(database, collection)

  private val jsonWriteSettings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build

  override protected[database] def put(d: DocumentAbstraction)(implicit transid: TransactionId): Future[DocInfo] = {
    //TODO: support bulk put
    val asJson = d.toDocumentRecord

    val id: String = asJson.fields("_id").convertTo[String].trim
    require(!id.isEmpty, "document id must be defined")

    val (old_rev, rev) = revisionCalculate(asJson)
    val docinfoStr = s"id: $id, rev: $rev"
    val start =
      transid.started(this, LoggingMarkers.DATABASE_SAVE, s"[PUT] '$collection' saving document: '${docinfoStr}'")

    val data = JsObject(asJson.fields + ("_rev" -> rev.toJson))
    val filters =
      if (rev.startsWith("1-")) {
        // for new document, we should get no matched document and insert new one
        // if there is a matched document, that one with no _rev filed will be replaced
        // if there is a document with the same id but has an _rev field, will return en E11000(conflict) error
        Filters.and(Filters.eq("_id", id), Filters.not(Filters.exists("_rev")))
      } else {
        // for old document, we should find a matched document and replace it
        // if no matched document find and try to insert new document, mongodb will return an E11000 error
        Filters.and(Filters.eq("_id", id), Filters.eq("_rev", old_rev))
      }

    val f =
      main_collection
        .findOneAndReplace(
          filters,
          Document(data.toString),
          FindOneAndReplaceOptions().upsert(true).returnDocument(ReturnDocument.AFTER))
        .toFuture()
        .map { doc =>
          transid.finished(this, start, s"[PUT] '$collection' completed document: '${docinfoStr}', document: '$doc'")
          DocInfo(DocId(id), DocRevision(rev))
        }
        .recover {
          case t: MongoException =>
            if (t.getCode == 11000) {
              transid.finished(this, start, s"[PUT] '$dbName', document: '${docinfoStr}'; conflict.")
              throw DocumentConflictException("conflict on 'put'")
            } else {
              transid.failed(
                this,
                start,
                s"[PUT] '$dbName' failed to put document: '${docinfoStr}'; return error code: '${t.getCode}'",
                ErrorLevel)
              throw new Exception("Unexpected mongodb server error: " + t.getMessage)
            }
        }

    reportFailure(
      f,
      failure =>
        transid
          .failed(this, start, s"[PUT] '$collection' internal error, failure: '${failure.getMessage}'", ErrorLevel))
  }

  override protected[database] def del(doc: DocInfo)(implicit transid: TransactionId): Future[Boolean] = {
    require(doc != null && doc.rev.asString != null, "doc revision required for delete")

    val start =
      transid.started(this, LoggingMarkers.DATABASE_DELETE, s"[DEL] '$collection' deleting document: '$doc'")

    val f = main_collection
      .deleteOne(Filters.and(Filters.eq("_id", doc.id.id), Filters.eq("_rev", doc.rev.rev)))
      .toFuture()
      .flatMap { result =>
        if (result.getDeletedCount == 1) {
          transid.finished(this, start, s"[DEL] '$collection' completed document: '$doc'")
          Future(true)
        } else {
          main_collection.find(Filters.eq("_id", doc.id.id)).toFuture.map { result =>
            if (result.size == 1) {
              // find the document according to _id, conflict
              transid.finished(this, start, s"[DEL] '$collection', document: '${doc}'; conflict.")
              throw DocumentConflictException("conflict on 'delete'")
            } else {
              // doesn't find the document according to _id, not found
              transid.finished(this, start, s"[DEL] '$collection', document: '${doc}'; not found.")
              throw NoDocumentException(s"${doc} not found on 'delete'")
            }
          }
        }
      }
      .recover {
        case t: MongoException =>
          transid.failed(
            this,
            start,
            s"[DEL] '$dbName' failed to delete document: '${doc}'; error code: '${t.getCode}'",
            ErrorLevel)
          throw new Exception("Unexpected mongodb server error: " + t.getMessage)
      }

    reportFailure(
      f,
      failure =>
        transid.failed(
          this,
          start,
          s"[DEL] '$dbName' internal error, doc: '$doc', failure: '${failure.getMessage}'",
          ErrorLevel))
  }

  override protected[database] def get[A <: DocumentAbstraction](doc: DocInfo)(implicit transid: TransactionId,
                                                                               ma: Manifest[A]): Future[A] = {

    val start = transid.started(this, LoggingMarkers.DATABASE_GET, s"[GET] '$dbName' finding document: '$doc'")

    require(doc != null, "doc undefined")

    val f = main_collection
      .find(Filters.eq("_id", doc.id.id))
      .toFuture()
      .map(result =>
        if (result.size == 0) {
          transid.finished(this, start, s"[GET] '$collection', document: '${doc}'; not found.")
          throw NoDocumentException("not found on 'get'")
        } else {
          transid.finished(this, start, s"[GET] '$collection' completed: found document '$doc'")
          deserialize[A, DocumentAbstraction](doc, result.head.toJson(jsonWriteSettings).parseJson.asJsObject)

      })
      .recoverWith {
        case e: DeserializationException => throw DocumentUnreadable(Messages.corruptedEntity)
      }

    reportFailure(
      f,
      failure =>
        transid.failed(
          this,
          start,
          s"[GET] '$collection' internal error, doc: '$doc', failure: '${failure.getMessage}'",
          ErrorLevel))
  }

  override protected[core] def query(table: String,
                                     startKey: List[Any],
                                     endKey: List[Any],
                                     skip: Int,
                                     limit: Int,
                                     includeDocs: Boolean,
                                     descending: Boolean,
                                     reduce: Boolean,
                                     stale: StaleParameter)(implicit transid: TransactionId): Future[List[JsObject]] = {

    //FIXME: staleParameter is meaningless in mongodb

    require(!(reduce && includeDocs), "reduce and includeDocs cannot both be true")
    require(skip >= 0, "skip should be non negative")
    require(limit >= 0, "limit should be non negative")

    val query_collection =
      if (reduce) database.getCollection(s"$collection.$table.reduce")
      else database.getCollection(s"$collection.$table")

    val start = transid.started(this, LoggingMarkers.DATABASE_QUERY, s"[QUERY] '$collection' searching '$table")

    val fields = if (includeDocs) IndexedSeq("id", "key", "value", "doc") else IndexedSeq("id", "key", "value")

    // the default mongodb sort order for array is different, for descending, it only compare the largest element, and
    // for ascending, it compare the smallest element, so for an array, we need compare its elements one by one
    val sort_fields = 0 to startKey.size map { "key." + _ }
    val sort = if (descending) Sorts.descending(sort_fields: _*) else Sorts.ascending(sort_fields: _*)

    val begin = if (startKey.nonEmpty) startKey else BsonMinKey()
    val end = if (endKey.nonEmpty) endKey else BsonMaxKey()
    val query = Filters.and(Filters.gte("key", begin), Filters.lte("key", end))

    val ob =
      if (reduce) {
        query_collection.aggregate(
          List(
            Aggregates.`match`(query),
            Aggregates.group(BsonNull(), Accumulators.sum("value", "$value")),
            Aggregates.project(Projections
              .fields(Projections.computed("key", "$_id"), Projections.excludeId(), Projections.include("value"))),
            Aggregates.limit(limit),
            Aggregates.skip(skip),
            Aggregates.sort(sort)))
      } else {
        query_collection
          .find(query)
          .limit(limit)
          .skip(skip)
          .projection(Projections.include(fields: _*))
          .sort(sort)
      }

    val f = ob.toFuture.map { results =>
      transid.finished(this, start, s"[QUERY] '$collection' completed: matched ${results.size}")
      results.map(result => result.toJson(jsonWriteSettings).parseJson.convertTo[JsObject]).toList
    }

    reportFailure(
      f,
      failure =>
        transid
          .failed(this, start, s"[QUERY] '$collection' internal error, failure: '${failure.getMessage}'", ErrorLevel))
  }

  protected[core] def count(table: String, startKey: List[Any], endKey: List[Any], skip: Int, stale: StaleParameter)(
    implicit transid: TransactionId): Future[Long] = {
    require(skip >= 0, "skip should be non negative")

    val start = transid.started(this, LoggingMarkers.DATABASE_QUERY, s"[COUNT] '$dbName' searching '$table")

    val query_collection = database.getCollection(s"$collection.$table")
    //it's available for the startKey being an empty list, while endKey is not
    val query =
      if (endKey.isEmpty) Filters.gte("key", startKey)
      else Filters.and(Filters.gte("key", startKey), Filters.lte("key", endKey))

    val option = CountOptions().skip(skip)
    val f =
      query_collection
        .count(query, option)
        .toFuture()
        .map { result =>
          transid.finished(this, start, s"[COUNT] '$collection' completed: count $result")
          result
        }

    reportFailure(
      f,
      failure =>
        transid
          .failed(this, start, s"[COUNT] '$dbName' internal error, failure: '${failure.getMessage}'", ErrorLevel))
  }

  override protected[core] def attach(
    doc: DocInfo,
    name: String,
    contentType: ContentType,
    docStream: Source[ByteString, _])(implicit transid: TransactionId): Future[DocInfo] = {

    val start = transid.started(
      this,
      LoggingMarkers.DATABASE_ATT_SAVE,
      s"[ATT_PUT] '$collection' uploading attachment '$name' of document '$doc'")

    require(doc != null, "doc undefined")

    val document: org.bson.Document = new org.bson.Document("contentType", contentType.mediaType.toString)
    contentType.charsetOption.foreach { charset =>
      document.append("charset", charset.value)
    }
    //add the document id to the metadata
    document.append("belongsTo", doc.id.id)

    val option = new GridFSUploadOptions().metadata(document)

    val uploadStream = gridFSBucket.openUploadStream(BsonString(doc.id.id + name), name, option)
    val inputStream: InputStream = docStream.runWith(StreamConverters.asInputStream())
    val b: Array[Byte] = IOUtils.toByteArray(inputStream)
    val f = uploadStream.write(ByteBuffer.wrap(b)).toFuture().map { _ =>
      transid
        .finished(this, start, s"[ATT_PUT] '$collection' completed uploading attachment '$name' of document '$doc'")
      uploadStream.close().toFuture().map { _ =>
        logging.debug(this, s"upload stream is closed")
      }
      // return the old docinfo as put attachment doesn't change anything in the original document
      doc
    }

    reportFailure(
      f,
      failure =>
        transid.failed(
          this,
          start,
          s"[ATT_PUT] '$collection' internal error, name: '$name', doc: '$doc', failure: '${failure.getMessage}'",
          ErrorLevel))
  }

  override protected[core] def readAttachment[T](doc: DocInfo, name: String, sink: Sink[ByteString, Future[T]])(
    implicit transid: TransactionId): Future[(ContentType, T)] = {

    val start = transid.started(
      this,
      LoggingMarkers.DATABASE_ATT_GET,
      s"[ATT_GET] '$dbName' finding attachment '$name' of document '$doc'")

    require(doc != null, "doc undefined")

    val dstByteBuffer = ByteBuffer.allocate(1024 * 1024)
    val downloadStream = gridFSBucket.openDownloadStream(BsonString(doc.id.id + name))
    val f = downloadStream.read(dstByteBuffer).toFuture().flatMap { result =>
      dstByteBuffer.flip()
      val bytes: Array[Byte] = new Array[Byte](result.head)
      dstByteBuffer.get(bytes)

      StreamConverters.fromInputStream(() => new ByteArrayInputStream(bytes)).runWith(sink).flatMap { r =>
        database.getCollection(s"$collection.files").find(Filters.eq("_id", doc.id.id + name)).first().toFuture().map {
          result =>
            transid.finished(this, start, s"[ATT_GET] '$dbName' completed get attachment '$name' of document '$doc'")
            val mongoFile = result.toJson(jsonWriteSettings).parseJson.convertTo[MongoFile]
            (translateContentType(mongoFile), r)
        }
      }
    }

    reportFailure(
      f,
      failure =>
        transid.failed(
          this,
          start,
          s"[ATT_GET] '$dbName' internal error, name: '$name', doc: '$doc', failure: '${failure.getMessage}'",
          ErrorLevel))
  }

  override protected[core] def deleteAttachments[T](doc: DocInfo)(implicit transid: TransactionId): Future[Boolean] = {
    val start = transid.started(
      this,
      LoggingMarkers.DATABASE_ATT_DELETE,
      s"[ATT_DELETE] '$dbName' delete attachments of document '$doc'")

    val f: Future[Boolean] =
      database
        .getCollection(s"$collection.files")
        .find(Filters.eq("metadata.belongsTo", doc.id.id))
        .projection(Projections.include("_id"))
        .toFuture
        .flatMap { documents =>
          Future
            .sequence(documents.map { doc =>
              gridFSBucket
                .delete(doc.getOrElse("_id", BsonString("")))
                .toFuture
                .map(Some(_))
                .fallbackTo(Future(None))
            })
            .map(_.flatten.flatten)
            .map { _ =>
              transid.finished(this, start, s"[ATT_DELETE] '$dbName' completed deleting attachment of document '$doc'")
              true
            }
        }

    reportFailure(
      f,
      failure =>
        transid.failed(
          this,
          start,
          s"[ATT_DELETE] '$dbName' internal error, doc: '$doc', failure: '${failure.getMessage}'",
          ErrorLevel))
  }

  override def shutdown(): Unit = {
    logging.info(this, "mongo-scala-driver doesn't need to manage connections explicitly")
  }

  private def reportFailure[T, U](f: Future[T], onFailure: Throwable => U): Future[T] = {
    f.onFailure({
      case _: ArtifactStoreException => // These failures are intentional and shouldn't trigger the catcher.
      case x                         => onFailure(x)
    })
    f
  }

  private def translateContentType(mongoFile: MongoFile): ContentType = {
    val contentType =
      s"${mongoFile.metadata.getOrElse("contentType", "None")}-${mongoFile.metadata.getOrElse("charset", "")}"
    contentType match {
      case "application/json"         => ContentTypes.`application/json`
      case "application/octet-stream" => ContentTypes.`application/octet-stream`
      case "text/plain-UTF-8"         => ContentTypes.`text/plain(UTF-8)`
      case "text/html-UTF-8"          => ContentTypes.`text/html(UTF-8)`
      case "text/xml-UTF-8"           => ContentTypes.`text/xml(UTF-8)`
      case "text/csv-UTF-8"           => ContentTypes.`text/csv(UTF-8)`
      case _                          => ContentTypes.NoContentType
    }
  }

  // calculate the revision manually, to be compatible with couchdb's _rev field
  private def revisionCalculate(doc: JsObject): (String, String) = {
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    val new_rev = md.digest(doc.toString.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }
    doc.fields
      .get("_rev")
      .map { value =>
        val start = value.convertTo[String].trim.split("-").apply(0).toInt + 1
        (value.convertTo[String].trim, s"$start-$new_rev")
      }
      .getOrElse {
        ("", s"1-$new_rev")
      }
  }
}
