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

import java.io.Closeable
import java.net.URLEncoder

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.mongodb.scala.MongoClient
import spray.json.RootJsonFormat
import whisk.common.Logging
import whisk.core.ConfigKeys
import whisk.core.database._
import whisk.core.entity.{DocumentReader, WhiskActivation, WhiskAuth, WhiskEntity}
import whisk.core.entity.size._
import pureconfig._

import scala.reflect.ClassTag

case class MongoDBConfig(hosts: String,
                         username: String,
                         replicaSet: String,
                         password: String,
                         database: String,
                         collections: Map[String, String]) {
  assume(Set(hosts, username, password).forall(_.nonEmpty), "At least one expected property is missing")

  def collectionFor[D](implicit tag: ClassTag[D]): String = {
    val entityType = tag.runtimeClass.getSimpleName
    collections.get(entityType) match {
      case Some(name) => name
      case None       => throw new IllegalArgumentException(s"Collection name mapping not found for $entityType")
    }
  }
}

case class ClientHolder(client: MongoClient) extends Closeable {
  override def close(): Unit = client.close()
}

object MongoDBClient {
  var _client: Option[MongoClient] = None

  def client(config: MongoDBConfig): MongoClient = {
    if (_client.isEmpty) {
      val username = URLEncoder.encode(config.username, "utf8")
      val password = URLEncoder.encode(config.password, "utf8")
      val uri =
        s"mongodb://${username}:${password}@${config.hosts}/?replicaSet=${config.replicaSet}&authSource=${config.database}"
      _client = Some(MongoClient(uri))
    }
    _client.get
  }
}

object MongoDBArtifactStoreProvider extends ArtifactStoreProvider {
  private val dbConfig = loadConfigOrThrow[MongoDBConfig](ConfigKeys.mongodb)
  type DocumentClientRef = ReferenceCounted[ClientHolder]#CountedReference
  private var clientRef: ReferenceCounted[ClientHolder] = _

  def makeStore[D <: DocumentSerializer: ClassTag](useBatching: Boolean)(
    implicit jsonFormat: RootJsonFormat[D],
    docReader: DocumentReader,
    actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): ArtifactStore[D] = {
    makeArtifactStore(getAttachmentStore())
  }

  def makeArtifactStore[D <: DocumentSerializer: ClassTag](attachmentStore: Option[AttachmentStore])(
    implicit jsonFormat: RootJsonFormat[D],
    docReader: DocumentReader,
    actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): ArtifactStore[D] = {

    val inliningConfig = loadConfigOrThrow[InliningConfig](ConfigKeys.db)

    val (handler, mapper) = handlerAndMapper(implicitly[ClassTag[D]])
    new MongoDBArtifactStore[D](
      getOrCreateReference(dbConfig),
      dbConfig.database,
      dbConfig.collectionFor[D],
      handler,
      mapper,
      inliningConfig,
      attachmentStore)
  }

  private def handlerAndMapper[D](entityType: ClassTag[D])(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): (DocumentHandler, MongoViewMapper) = {
    entityType.runtimeClass match {
      case x if x == classOf[WhiskEntity] =>
        (WhisksHandler, WhisksViewMapper)
      case x if x == classOf[WhiskActivation] =>
        (ActivationHandler, ActivationViewMapper)
      case x if x == classOf[WhiskAuth] =>
        (SubjectHandler, SubjectViewMapper)
    }
  }

  /*
   * This method ensures that all store instances share same client instance and thus the underlying connection pool.
   * Synchronization is required to ensure concurrent init of various store instances share same ref instance
   */
  private def getOrCreateReference(config: MongoDBConfig) = synchronized {
    if (clientRef == null || clientRef.isClosed) {
      clientRef = createReference(config)
    }
    clientRef.reference()
  }

  private def createReference(config: MongoDBConfig) = {
    new ReferenceCounted[ClientHolder](ClientHolder(MongoDBClient.client(dbConfig)))
  }
}
