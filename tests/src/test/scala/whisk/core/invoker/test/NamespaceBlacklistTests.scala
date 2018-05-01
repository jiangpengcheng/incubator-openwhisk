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

package whisk.core.invoker.test

import akka.stream.ActorMaterializer
import common.{StreamLogging, WskActorSystem}
import org.junit.runner.RunWith
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}
import spray.json.DefaultJsonProtocol._
import spray.json._
import whisk.common.TransactionId
import whisk.core.database.StaleParameter
import whisk.core.database.test.DbUtils
import whisk.core.entity._
import whisk.core.entity.types.AuthStore
import whisk.core.invoker.NamespaceBlacklist
import whisk.utils.{retry => testRetry}

import scala.concurrent.duration._

class LimitEntity(name: EntityName, limits: UserLimits) extends WhiskAuth(Subject(), Set.empty) {
  override def docid = DocId(s"${name.name}/limits")

  //There is no api to write limits. So piggy back on WhiskAuth but replace auth json
  //with limits!
  override def toJson = UserLimits.serdes.write(limits).asJsObject
}

class ExtendedAuth(subject: Subject, namespaces: Set[WhiskNamespace], blocked: Boolean)
    extends WhiskAuth(subject, namespaces) {
  override def toJson = JsObject(super.toJson.fields + ("blocked" -> JsBoolean(blocked)))
}

@RunWith(classOf[JUnitRunner])
class NamespaceBlacklistTests
    extends FlatSpec
    with Matchers
    with DbUtils
    with ScalaFutures
    with IntegrationPatience
    with WskActorSystem
    with StreamLogging {

  behavior of "NamespaceBlacklist"

  implicit val materializer = ActorMaterializer()
  implicit val tid = TransactionId.testing

  val authStore = WhiskAuthStore.datastore()

  val limits = Seq(
    new LimitEntity(EntityName("testnamespace1"), UserLimits(invocationsPerMinute = Some(0))),
    new LimitEntity(EntityName("testnamespace2"), UserLimits(concurrentInvocations = Some(0))),
    new LimitEntity(
      EntityName("testnamespace3"),
      UserLimits(invocationsPerMinute = Some(1), concurrentInvocations = Some(1))))

  val extendAuth = new ExtendedAuth(
    Subject(),
    Set(WhiskNamespace(EntityName("different1"), AuthKey()), WhiskNamespace(EntityName("different2"), AuthKey())),
    blocked = true)

  val blockedNamespacesCount = 2 + extendAuth.namespaces.size

  def authToIdentities(auth: WhiskAuth): Set[Identity] = {
    auth.namespaces.map { ns =>
      Identity(auth.subject, ns.name, ns.authkey, Set(), UserLimits())
    }
  }

  def limitToIdentity(limit: LimitEntity): Identity = {
    val namespace = limit.docid.id.dropRight(7)
    Identity(limit.subject, EntityName(namespace), AuthKey(), Set(), UserLimits())
  }

  override def beforeAll() = {
    limits.foreach(put(authStore, _))
    put(authStore, extendAuth)

    waitOnBlacklistView(authStore, blockedNamespacesCount)
  }

  override def afterAll() = {
    cleanup()
  }

  it should "mark a namespace as blocked if limit is 0 in database or if one of its subjects is blocked" in {
    val blacklist = new NamespaceBlacklist(authStore)

    testRetry({
      blacklist.refreshBlacklist().futureValue should have size blockedNamespacesCount
    }, 60, Some(1.second))

    limits.map(limitToIdentity).map(blacklist.isBlacklisted) shouldBe Seq(true, true, false)
    authToIdentities(extendAuth).toSeq.map(blacklist.isBlacklisted) shouldBe Seq(true, true)
  }

  def waitOnBlacklistView(db: AuthStore, count: Int)(implicit timeout: Duration) = {
    val success = retry(() => {
      blacklistCount().map { listCount =>
        if (listCount != count) {
          throw RetryOp()
        } else true
      }
    }, timeout)
    assert(success.isSuccess, "wait aborted after: " + timeout + ": " + success)
  }

  private def blacklistCount() = {
    //NamespaceBlacklist uses StaleParameter.UpdateAfter which would lead to race condition
    //So use actual call here
    authStore
      .query(
        table = NamespaceBlacklist.view.name,
        startKey = List.empty,
        endKey = List.empty,
        skip = 0,
        limit = Int.MaxValue,
        includeDocs = false,
        descending = true,
        reduce = false,
        stale = StaleParameter.No)
      .map(_.map(_.fields("key").convertTo[String]).toSet)
      .map(_.size)
  }

}
