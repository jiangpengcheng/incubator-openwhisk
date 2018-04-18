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

package whisk.core.database

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.ByteBuffer

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
import org.mongodb.scala.bson.BsonString
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.gridfs.GridFSBucket
import org.mongodb.scala.model._
import org.mongodb.scala.MongoClient
import spray.json._
import whisk.common.{Logging, LoggingMarkers, TransactionId}
import whisk.core.entity._

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
    val asJson = d.toDocumentRecord

    val id: String = asJson.fields("_id").convertTo[String].trim
    require(!id.isEmpty, "document id must be defined")

    val docinfoStr = s"id: $id"
    val start =
      transid.started(this, LoggingMarkers.DATABASE_SAVE, s"[PUT] '$collection' saving document: '${docinfoStr}'")

    val f =
      main_collection
        .findOneAndReplace(Filters.eq("_id", id), Document(asJson.toString), FindOneAndReplaceOptions().upsert(true))
        .toFuture()
        .map { doc =>
          transid.finished(this, start, s"[PUT] '$collection' completed document: '${docinfoStr}', document: '$doc'")
          DocInfo(DocId(id))
        }

    reportFailure(
      f,
      failure =>
        transid
          .failed(this, start, s"[PUT] '$collection' internal error, failure: '${failure.getMessage}'", ErrorLevel))
  }

  override protected[database] def del(doc: DocInfo)(implicit transid: TransactionId): Future[Boolean] = {
    require(doc != null, "doc info required for delete")

    val start =
      transid.started(this, LoggingMarkers.DATABASE_DELETE, s"[DEL] '$collection' deleting document: '$doc'")

    val f = main_collection.deleteOne(Filters.eq("_id", doc.id.id)).toFuture().map { result =>
      if (result.getDeletedCount == 1) {
        transid.finished(this, start, s"[DEL] '$collection' completed document: '$doc'")
        true
      } else if (result.getDeletedCount == 0) {
        transid.finished(this, start, s"[DEL] '$collection', document: '${doc}'; not found.")
        throw NoDocumentException(s"${doc} not found on 'delete'")
      } else {
        //this should not happen as we use the primary key to filter
        transid.finished(this, start, s"[DEL] '$collection', document: '${doc}'; multi documents are deleted.")
        throw MultiDocumentsMatchedException(s"multi documents: ${doc} are 'delete'")
      }
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
      .map(
        result =>
          result.headOption
            .map(doc => {
              transid.finished(this, start, s"[GET] '$collection' completed: found document '$doc'")
              docReader.read(ma, doc.toJson(jsonWriteSettings).parseJson).asInstanceOf[A]
            })
            .getOrElse {
              transid.finished(this, start, s"[GET] '$collection' failed to get document: '$doc'")
              throw NoDocumentException(s"${doc} not found on 'get'")
          })

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

    val query_collection =
      if (reduce) database.getCollection(s"$collection.$table.reduce")
      else database.getCollection(s"$collection.$table")

    val start = transid.started(this, LoggingMarkers.DATABASE_QUERY, s"[QUERY] '$collection' searching '$table")

    val fields = if (includeDocs) IndexedSeq("id", "key", "value", "doc") else IndexedSeq("id", "key", "value")
    val sort = if (descending) Sorts.descending("key") else Sorts.ascending("key")

    //it's available for the startKey being an empty list, while endKey is not
    val query =
      if (endKey.isEmpty) Filters.gte("key", startKey)
      else Filters.and(Filters.gte("key", startKey), Filters.lte("key", endKey))

    val ob =
      if (reduce) {
        query_collection.aggregate(
          List(
            Aggregates.`match`(query),
            Aggregates.group("", Accumulators.sum("value", "$value")),
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
      DocInfo(DocId(uploadStream.id.asString.getValue), DocRevision.empty)
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
    client.close()
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
}
