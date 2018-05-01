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

import java.net.URLEncoder

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.mongodb.scala.MongoClient
import spray.json.RootJsonFormat
import whisk.common.Logging
import whisk.core.ConfigKeys
import whisk.core.database._
import whisk.core.entity.{DocumentReader, WhiskActivation, WhiskAuth, WhiskEntity}
import pureconfig._

import scala.reflect.ClassTag

case class MongoDbConfig(hosts: String,
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

class ClientNotInitException extends Exception {
  override def toString: String = {
    "The MongoDB client is not inited"
  }
}

object MongoDbClient {
  var _client: Option[MongoClient] = None

  def setup(config: MongoDbConfig): Unit = {
    if (_client.isEmpty) {
      val username = URLEncoder.encode(config.username, "utf8")
      val password = URLEncoder.encode(config.password, "utf8")
      val uri =
        s"mongodb://${username}:${password}@${config.hosts}/?replicaSet=${config.replicaSet}&authSource=${config.database}"
      _client = Some(MongoClient(uri))
    }
  }

  def client: MongoClient = {
    _client.getOrElse(throw new ClientNotInitException)
  }
}

object MongoDbStoreProvider extends ArtifactStoreProvider {
  private val dbConfig = loadConfigOrThrow[MongoDbConfig](ConfigKeys.mongodb)

  def makeStore[D <: DocumentSerializer: ClassTag](useBatching: Boolean)(
    implicit jsonFormat: RootJsonFormat[D],
    docReader: DocumentReader,
    actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): ArtifactStore[D] = {

    MongoDbClient.setup(dbConfig)
    val (handler, mapper) = handlerAndMapper(implicitly[ClassTag[D]], MongoDbClient.client)
    new MongoDbStore[D](
      MongoDbClient.client,
      dbConfig.database,
      dbConfig.collectionFor[D],
      handler,
      mapper,
      useBatching)
  }

  private def handlerAndMapper[D](entityType: ClassTag[D], client: MongoClient)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): (DocumentHandler, MongoViewMapper) = {
    val db = client.getDatabase(dbConfig.database)
    entityType.runtimeClass match {
      case x if x == classOf[WhiskEntity] =>
        (WhisksHandler, WhisksViewMapper)
      case x if x == classOf[WhiskActivation] =>
        (ActivationHandler, ActivationViewMapper)
      case x if x == classOf[WhiskAuth] =>
        (SubjectHandler, SubjectViewMapper)
    }
  }
}
