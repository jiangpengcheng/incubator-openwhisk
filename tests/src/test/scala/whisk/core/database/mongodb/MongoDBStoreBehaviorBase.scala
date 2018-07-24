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

import org.scalatest.FlatSpec
import pureconfig.loadConfigOrThrow
import whisk.core.ConfigKeys
import whisk.core.database.test.behavior.ArtifactStoreBehaviorBase
import whisk.core.database.{ArtifactStore, AttachmentStore, DocumentSerializer}
import whisk.core.database.test.behavior.ArtifactStoreTestUtil.storeAvailable
import whisk.core.entity.{
  DocumentReader,
  WhiskActivation,
  WhiskAuth,
  WhiskDocumentReader,
  WhiskEntity,
  WhiskEntityJsonFormat
}

import scala.reflect.{classTag, ClassTag}
import scala.util.Try

trait MongoDBStoreBehaviorBase extends FlatSpec with ArtifactStoreBehaviorBase {
  override def storeType = "MongoDB"

  override lazy val storeAvailableCheck: Try[Any] = storeConfigTry

  private def storeConfig = storeConfigTry.get

  override lazy val authStore = {
    implicit val docReader: DocumentReader = WhiskDocumentReader
    MongoDBArtifactStoreProvider.makeArtifactStore[WhiskAuth](storeConfig, getAttachmentStore[WhiskAuth]())
  }

  override lazy val entityStore =
    MongoDBArtifactStoreProvider.makeArtifactStore[WhiskEntity](storeConfig, getAttachmentStore[WhiskEntity]())(
      classTag[WhiskEntity],
      WhiskEntityJsonFormat,
      WhiskDocumentReader,
      actorSystem,
      logging,
      materializer)

  override lazy val activationStore = {
    implicit val docReader: DocumentReader = WhiskDocumentReader
    MongoDBArtifactStoreProvider
      .makeArtifactStore[WhiskActivation](storeConfig, getAttachmentStore[WhiskActivation]())
  }

  override protected def getAttachmentStore(store: ArtifactStore[_]) =
    store.asInstanceOf[MongoDBArtifactStore[_]].attachmentStore

  protected def getAttachmentStore[D <: DocumentSerializer: ClassTag](): Option[AttachmentStore] = None

  private lazy val storeConfigTry = Try { loadConfigOrThrow[MongoDBConfig](ConfigKeys.mongodb) }

  override protected def withFixture(test: NoArgTest) = {
    assume(storeAvailable(storeConfigTry), "MongoDB not configured or available")
    super.withFixture(test)
  }
}
