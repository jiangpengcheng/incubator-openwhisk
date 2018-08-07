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

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import whisk.common.Logging
import whisk.core.database._
import whisk.core.entity._
import scala.reflect.classTag

class MongoDBActivationStore(actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, logging: Logging)
    extends ArtifactActivationStore(actorSystem, actorMaterializer, logging) {

  protected override val artifactStore: ArtifactStore[WhiskActivation] =
    MongoDBArtifactStoreProvider.makeStore[WhiskActivation](useBatching = false)(
      classTag[WhiskActivation],
      WhiskActivation.serdes,
      WhiskDocumentReader,
      actorSystem,
      logging,
      actorMaterializer)

}

object MongoDBActivationStoreProvider extends ActivationStoreProvider {
  override def instance(actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, logging: Logging) =
    new MongoDBActivationStore(actorSystem, actorMaterializer, logging)
}
