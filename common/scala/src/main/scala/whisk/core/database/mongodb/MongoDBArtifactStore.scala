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

import java.security.MessageDigest

import akka.actor.ActorSystem
import akka.event.Logging.ErrorLevel
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString
import com.mongodb.client.gridfs.model.GridFSUploadOptions
import org.bson.json.{JsonMode, JsonWriterSettings}
import org.mongodb.scala.bson.BsonString
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.gridfs.{GridFSBucket, GridFSFile, MongoGridFSException}
import org.mongodb.scala.model._
import org.mongodb.scala.MongoException
import spray.json._
import whisk.common.{Logging, LoggingMarkers, TransactionId}
import whisk.core.database._
import whisk.core.entity._
import whisk.core.database.StoreUtils._
import whisk.core.database.mongodb.MongoDBArtifactStoreProvider.DocumentClientRef
import whisk.core.entity.Attachments.Attached
import whisk.http.Messages

import scala.concurrent.Future
import scala.util.Try

object MongoDBArtifactStore {
  val _computed = "_computed"
  implicit class StringPurge(val s: String) {
    def escapeDollar: String = {
      // replace '"$xxx":' to '"_mark_of_dollar_xxx":' as the "$" can not be the first char of field name
      s.replaceAll("\"\\$([^}]+?\":)", "\"_mark_of_dollar_$1")
    }

    def recoverDollar: String = {
      // restore the "$" from "_mark_of_dollar_"
      s.replaceAll("\"_mark_of_dollar_([^}]+?\":)", "\"\\$$1")
    }
  }
}

/**
 * Basic client to put and delete artifacts in a data store.
 *
 * @param clientRef the mongodb client reference to access database
 * @param dbName the name of the database to operate on
 * @param collection the name of the collection to operate on
 * @param documentHandler helper class help to simulate the designDoc of CouchDB
 * @param viewMapper helper class help to simulate the designDoc of CouchDB
 */
class MongoDBArtifactStore[DocumentAbstraction <: DocumentSerializer](clientRef: DocumentClientRef,
                                                                      dbName: String,
                                                                      collection: String,
                                                                      documentHandler: DocumentHandler,
                                                                      viewMapper: MongoViewMapper,
                                                                      val inliningConfig: InliningConfig,
                                                                      val attachmentStore: Option[AttachmentStore])(
  implicit system: ActorSystem,
  val logging: Logging,
  jsonFormat: RootJsonFormat[DocumentAbstraction],
  val materializer: ActorMaterializer,
  docReader: DocumentReader)
    extends ArtifactStore[DocumentAbstraction]
    with DocumentProvider
    with DefaultJsonProtocol
    with AttachmentSupport[DocumentAbstraction] {

  import whisk.core.database.mongodb.MongoDBArtifactStore._

  protected[core] implicit val executionContext = system.dispatcher

  private val mongodbScheme = "mongodb"
  val attachmentScheme: String = attachmentStore.map(_.scheme).getOrElse(mongodbScheme)

  private val client = clientRef.get.client

  private val database = client.getDatabase(dbName)
  private val main_collection = database.getCollection(collection)
  private val gridFSBucket = GridFSBucket(database, collection)

  private val jsonWriteSettings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build

  override protected[database] def put(d: DocumentAbstraction)(implicit transid: TransactionId): Future[DocInfo] = {
    val asJson = d.toDocumentRecord

    val id: String = asJson.fields("_id").convertTo[String].trim
    require(!id.isEmpty, "document id must be defined")

    val (old_rev, rev) = revisionCalculate(asJson)
    val docinfoStr = s"id: $id, rev: $rev"
    val start =
      transid.started(this, LoggingMarkers.DATABASE_SAVE, s"[PUT] '$collection' saving document: '$docinfoStr'")

    val data = JsObject(asJson.fields + (_computed -> documentHandler.computedFields(asJson)) + ("_rev" -> rev.toJson))
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
          Document(data.compactPrint.escapeDollar),
          FindOneAndReplaceOptions().upsert(true).returnDocument(ReturnDocument.AFTER))
        .toFuture()
        .map { doc =>
          transid.finished(this, start, s"[PUT] '$collection' completed document: '$docinfoStr', document: '$doc'")
          DocInfo(DocId(id), DocRevision(rev))
        }
        .recover {
          case t: MongoException if t.getCode == 11000 =>
            transid.finished(this, start, s"[PUT] '$dbName', document: '$docinfoStr'; conflict.")
            throw DocumentConflictException("conflict on 'put'")
          case t: MongoException =>
            transid.failed(
              this,
              start,
              s"[PUT] '$dbName' failed to put document: '$docinfoStr'; return error code: '${t.getCode}'",
              ErrorLevel)
            throw new Exception("Unexpected mongodb server error: " + t.getMessage)
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
        if (result.getDeletedCount == 1) { // the result can only be 1 or 0
          transid.finished(this, start, s"[DEL] '$collection' completed document: '$doc'")
          Future(true)
        } else {
          main_collection.find(Filters.eq("_id", doc.id.id)).toFuture.map { result =>
            if (result.size == 1) {
              // find the document according to _id, conflict
              transid.finished(this, start, s"[DEL] '$collection', document: '$doc'; conflict.")
              throw DocumentConflictException("conflict on 'delete'")
            } else {
              // doesn't find the document according to _id, not found
              transid.finished(this, start, s"[DEL] '$collection', document: '$doc'; not found.")
              throw NoDocumentException(s"$doc not found on 'delete'")
            }
          }
        }
      }
      .recover {
        case t: MongoException =>
          transid.failed(
            this,
            start,
            s"[DEL] '$collection' failed to delete document: '$doc'; error code: '${t.getCode}'",
            ErrorLevel)
          throw new Exception("Unexpected mongodb server error: " + t.getMessage)
      }

    reportFailure(
      f,
      failure =>
        transid.failed(
          this,
          start,
          s"[DEL] '$collection' internal error, doc: '$doc', failure: '${failure.getMessage}'",
          ErrorLevel))
  }

  override protected[database] def get[A <: DocumentAbstraction](doc: DocInfo,
                                                                 attachmentHandler: Option[(A, Attached) => A] = None)(
    implicit transid: TransactionId,
    ma: Manifest[A]): Future[A] = {

    val start = transid.started(this, LoggingMarkers.DATABASE_GET, s"[GET] '$dbName' finding document: '$doc'")

    require(doc != null, "doc undefined")

    val f = main_collection
      .find(Filters.eq("_id", doc.id.id)) // method deserialize will check whether the _rev matched
      .toFuture()
      .map(result =>
        if (result.isEmpty) {
          transid.finished(this, start, s"[GET] '$collection', document: '$doc'; not found.")
          throw NoDocumentException("not found on 'get'")
        } else {
          transid.finished(this, start, s"[GET] '$collection' completed: found document '$doc'")
          val response = result.head.toJson(jsonWriteSettings).recoverDollar.parseJson.asJsObject
          val deserializedDoc = deserialize[A, DocumentAbstraction](doc, response)
          attachmentHandler
            .map(processAttachments(deserializedDoc, response, doc.id.id, _))
            .getOrElse(deserializedDoc)
      })
      .recoverWith {
        case t: MongoException =>
          transid.finished(
            this,
            start,
            s"[GET] '$collection' failed to get document: '$doc'; error code: '${t.getCode}'")
          throw new Exception("Unexpected mongodb server error: " + t.getMessage)
        case _: DeserializationException => throw DocumentUnreadable(Messages.corruptedEntity)
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

  override protected[database] def get(id: DocId)(implicit transid: TransactionId): Future[Option[JsObject]] = {
    val start = transid.started(this, LoggingMarkers.DATABASE_GET, s"[GET] '$collection' finding document: '$id'")
    val f = main_collection
      .find(Filters.equal("_id", id.id))
      .head()
      .map {
        case d: Document =>
          transid.finished(this, start, s"[GET] '$dbName' completed: found document '$id'")
          Some(d.toJson(jsonWriteSettings).recoverDollar.parseJson.asJsObject)
        case null =>
          transid.finished(this, start, s"[GET] '$dbName', document: '$id'; not found.")
          None
      }
      .recover {
        case t: MongoException =>
          transid.failed(
            this,
            start,
            s"[GET] '$collection' failed to get document: '$id'; error code: '${t.getCode}'",
            ErrorLevel)
          throw new Exception("Unexpected mongodb server error: " + t.getMessage)
      }

    reportFailure(
      f,
      failure =>
        transid.failed(
          this,
          start,
          s"[GET] '$collection' internal error, doc: '$id', failure: '${failure.getMessage}'",
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
    require(!(reduce && includeDocs), "reduce and includeDocs cannot both be true")
    require(!reduce, "Reduce scenario not supported") //TODO Investigate reduce
    require(skip >= 0, "skip should be non negative")
    require(limit >= 0, "limit should be non negative")

    val Array(ddoc, viewName) = table.split("/")

    val find = main_collection
      .find(viewMapper.filter(ddoc, viewName, startKey, endKey))

    viewMapper.sort(ddoc, viewName, descending).foreach(find.sort)

    find.skip(skip).limit(limit)

    val realIncludeDocs = includeDocs | documentHandler.shouldAlwaysIncludeDocs(ddoc, viewName)
    val start = transid.started(this, LoggingMarkers.DATABASE_QUERY, s"[QUERY] '$collection' searching '$table")

    val f = find
      .toFuture()
      .map { docs =>
        transid.finished(this, start, s"[QUERY] '$dbName' completed: matched ${docs.size}")
        docs.map { doc =>
          val js = doc.toJson(jsonWriteSettings).recoverDollar.parseJson.convertTo[JsObject]
          documentHandler.transformViewResult(
            ddoc,
            viewName,
            startKey,
            endKey,
            realIncludeDocs,
            JsObject(js.fields - _computed),
            MongoDBArtifactStore.this)
        }
      }
      .flatMap(Future.sequence(_))
      .map(_.flatten.toList)
      .recover {
        case t: MongoException =>
          transid.failed(this, start, s"[QUERY] '$collection' failed; error code: '${t.getCode}'", ErrorLevel)
          throw new Exception("Unexpected mongodb server error: " + t.getMessage)
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

    val Array(ddoc, viewName) = table.split("/")
    val start = transid.started(this, LoggingMarkers.DATABASE_QUERY, s"[COUNT] '$dbName' searching '$table")

    val query = viewMapper.filter(ddoc, viewName, startKey, endKey)

    val option = CountOptions().skip(skip)
    val f =
      main_collection
        .count(query, option)
        .toFuture()
        .map { result =>
          transid.finished(this, start, s"[COUNT] '$collection' completed: count $result")
          result
        }
        .recover {
          case t: MongoException =>
            transid.failed(this, start, s"[COUNT] '$collection' failed; error code: '${t.getCode}'", ErrorLevel)
            throw new Exception("Unexpected mongodb server error: " + t.getMessage)
        }

    reportFailure(
      f,
      failure =>
        transid
          .failed(this, start, s"[COUNT] '$dbName' internal error, failure: '${failure.getMessage}'", ErrorLevel))
  }

  override protected[database] def putAndAttach[A <: DocumentAbstraction](
    doc: A,
    update: (A, Attached) => A,
    contentType: ContentType,
    docStream: Source[ByteString, _],
    oldAttachment: Option[Attached])(implicit transid: TransactionId): Future[(DocInfo, Attached)] = {

    attachmentStore match {
      case Some(as) =>
        attachToExternalStore(doc, update, contentType, docStream, oldAttachment, as)
      case None =>
        attachToMongo(doc, update, contentType, docStream, oldAttachment)
    }

  }

  private def attachToMongo[A <: DocumentAbstraction](
    doc: A,
    update: (A, Attached) => A,
    contentType: ContentType,
    docStream: Source[ByteString, _],
    oldAttachment: Option[Attached])(implicit transid: TransactionId): Future[(DocInfo, Attached)] = {

    for {
      bytesOrSource <- inlineOrAttach(docStream)
      uri = uriOf(bytesOrSource, UUID().asString)
      attached <- {
        bytesOrSource match {
          case Left(bytes) =>
            Future.successful(Attached(uri.toString, contentType, Some(bytes.size), Some(digest(bytes))))
          case Right(source) =>
            attach(doc, uri.path.toString, contentType, source).map { r =>
              Attached(uri.toString, contentType, Some(r.length), Some(r.digest))
            }
        }
      }
      docInfo <- put(update(doc, attached))

      //Remove old attachment if it was part of attachmentStore
      _ <- oldAttachment
        .map { old =>
          val oldUri = Uri(old.attachmentName)
          if (oldUri.scheme == mongodbScheme) {
            val name = oldUri.path.toString
            gridFSBucket.delete(BsonString(s"${docInfo.id.id}/$name")).toFuture.map { _ =>
              true
            }
          } else {
            Future.successful(true)
          }
        }
        .getOrElse(Future.successful(true))
    } yield (docInfo, attached)
  }

  private def attach(d: DocumentAbstraction, name: String, contentType: ContentType, docStream: Source[ByteString, _])(
    implicit transid: TransactionId): Future[AttachResult] = {

    val asJson = d.toDocumentRecord
    val id: String = asJson.fields("_id").convertTo[String].trim
    require(!id.isEmpty, "document id must be defined")

    val start = transid.started(
      this,
      LoggingMarkers.DATABASE_ATT_SAVE,
      s"[ATT_PUT] '$collection' uploading attachment '$name' of document 'id: $id'")

    val document: org.bson.Document = new org.bson.Document("contentType", contentType.toString)
    //add the document id to the metadata
    document.append("belongsTo", id)

    val option = new GridFSUploadOptions().metadata(document)

    val uploadStream = gridFSBucket.openUploadStream(BsonString(s"$id/$name"), name, option)
    val sink = AsyncStreamSink(uploadStream)
    val f = docStream
      .runWith(combinedSink(sink))
      .map { r =>
        transid
          .finished(this, start, s"[ATT_PUT] '$collection' completed uploading attachment '$name' of document '$id'")
        AttachResult(r.digest, r.length)
      }
      .recover {
        case t: MongoException =>
          transid.failed(
            this,
            start,
            s"[ATT_PUT] '$collection' failed to upload attachment '$name' of document '$id'; error code '${t.getCode}'",
            ErrorLevel)
          throw new Exception("Unexpected mongodb server error: " + t.getMessage)
      }

    reportFailure(
      f,
      failure =>
        transid.failed(
          this,
          start,
          s"[ATT_PUT] '$collection' internal error, name: '$name', doc: '$id', failure: '${failure.getMessage}'",
          ErrorLevel))
  }

  override protected[core] def readAttachment[T](doc: DocInfo, attached: Attached, sink: Sink[ByteString, Future[T]])(
    implicit transid: TransactionId): Future[T] = {

    val name = attached.attachmentName
    val attachmentUri = Uri(name)

    attachmentUri.scheme match {
      case AttachmentSupport.MemScheme =>
        memorySource(attachmentUri).runWith(sink)
      case s if s == mongodbScheme || attachmentUri.isRelative =>
        //relative case is for compatibility with earlier naming approach where attachment name would be like 'jarfile'
        //Compared to current approach of '<scheme>:<name>'
        readAttachmentFromMongo(doc, attachmentUri, sink)
      case s if attachmentStore.isDefined && attachmentStore.get.scheme == s =>
        attachmentStore.get.readAttachment(doc.id, attachmentUri.path.toString, sink)
      case _ =>
        throw new IllegalArgumentException(s"Unknown attachment scheme in attachment uri $attachmentUri")
    }
  }

  private def readAttachmentFromMongo[T](doc: DocInfo, attachmentUri: Uri, sink: Sink[ByteString, Future[T]])(
    implicit transid: TransactionId): Future[T] = {

    val attachmentName = attachmentUri.path.toString
    val start = transid.started(
      this,
      LoggingMarkers.DATABASE_ATT_GET,
      s"[ATT_GET] '$dbName' finding attachment '$attachmentName' of document '$doc'")

    require(doc != null, "doc undefined")
    require(doc.rev.rev != null, "doc revision must be specified")

    val downloadStream = gridFSBucket.openDownloadStream(BsonString(s"${doc.id.id}/$attachmentName"))

    def readStream(file: GridFSFile) = {
      val source = AsyncStreamSource(downloadStream)
      source
        .runWith(sink)
        .map { result =>
          transid
            .finished(
              this,
              start,
              s"[ATT_GET] '$collection' completed: found attachment '$attachmentName' of document '$doc'")
          result
        }
    }

    def getGridFSFile = {
      downloadStream
        .gridFSFile()
        .head()
        .transform(
          identity, {
            case ex: MongoGridFSException if ex.getMessage.contains("File not found") =>
              transid.finished(
                this,
                start,
                s"[ATT_GET] '$collection', retrieving attachment '$attachmentName' of document '$doc'; not found.")
              NoDocumentException("Not found on 'readAttachment'.")
            case ex: MongoGridFSException =>
              transid.failed(
                this,
                start,
                s"[ATT_GET] '$collection' failed to get attachment '$attachmentName' of document '$doc'; error code: '${ex.getCode}'",
                ErrorLevel)
              throw new Exception("Unexpected mongodb server error: " + ex.getMessage)
            case t => t
          })
    }

    val f = for {
      file <- getGridFSFile
      result <- readStream(file)
    } yield result

    reportFailure(
      f,
      failure =>
        transid.failed(
          this,
          start,
          s"[ATT_GET] '$dbName' internal error, name: '$attachmentName', doc: '$doc', failure: '${failure.getMessage}'",
          ErrorLevel))

  }

  override protected[core] def deleteAttachments[T](doc: DocInfo)(implicit transid: TransactionId): Future[Boolean] =
    attachmentStore
      .map(as => as.deleteAttachments(doc.id))
      .getOrElse(Future.successful(true)) // For MongoDB it is expected that the entire document is deleted.

  override def shutdown(): Unit = {
    clientRef.close()
    attachmentStore.foreach(_.shutdown())
  }

  private def reportFailure[T, U](f: Future[T], onFailure: Throwable => U): Future[T] = {
    f.onFailure({
      case _: ArtifactStoreException => // These failures are intentional and shouldn't trigger the catcher.
      case x                         => onFailure(x)
    })
    f
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

  private def processAttachments[A <: DocumentAbstraction](doc: A,
                                                           js: JsObject,
                                                           docId: String,
                                                           attachmentHandler: (A, Attached) => A): A = {
    js.fields("exec").asJsObject().fields.get("code").map {
      case code: JsObject =>
        code.getFields("attachmentName", "attachmentType", "digest", "length") match {
          case Seq(JsString(name), JsString(contentTypeValue), JsString(digest), JsNumber(length)) =>
            val contentType = ContentType.parse(contentTypeValue) match {
              case Right(ct) => ct
              case Left(_)   => ContentTypes.NoContentType //Should not happen
            }
            attachmentHandler(
              doc,
              Attached(getAttachmentName(name), contentType, Some(length.longValue()), Some(digest)))
          case x =>
            throw DeserializationException("Attachment json does not have required fields" + x)
        }
      case _ => doc
    } getOrElse {
      doc // This should not happen as an action always contain field: exec.code
    }
  }

  /**
   * Determines if the attachment scheme confirms to new UUID based scheme or not
   * and generates the name based on that
   */
  private def getAttachmentName(name: String): String = {
    Try(java.util.UUID.fromString(name))
      .map(_ => Uri.from(scheme = attachmentScheme, path = name).toString)
      .getOrElse(name)
  }
}
