/*
 * Copyright (C) 2014 Egemen Kalyoncu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.vangas.cassandra.connection

import java.net.InetSocketAddress
import java.util.concurrent.TimeoutException

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import net.vangas.cassandra.CassandraConstants._
import net.vangas.cassandra._
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.message._
import org.scalatest.Matchers._
import org.scalatest._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DefaultSessionSpec extends TestKit(ActorSystem("DefaultSessionSystem"))
  with FunSpecLike with BeforeAndAfterAll with OneInstancePerTest {

  val router = TestProbe()
  val session = new DefaultSession("SESSION_KS", Configuration(), router.ref)

  override def afterAll() {
    system.shutdown()
  }

  describe("DefaultSession") {
    it("should execute simple query and return resultset") {
      val future = session.execute("simple_query")
      router.expectMsgPF(){ case Query("simple_query", _) => true }
      router.reply(Result(Void))
      val result = Await.result(future, 1 second)
      result.size should be(0)
    }

    it("should timeout when server doesn't return in time") {
      val future = session.execute("simple_query")
      router.expectMsgPF(){ case Query("simple_query", _) => true }
      intercept[TimeoutException] { Await.result(future, 1 second) }
    }

    it("should retry unprepared statement") {
      val node = new InetSocketAddress("localhost", 1234)
      val query = "prepare_query1"
      val queryId = new PreparedId("id".getBytes)
      val prepared = new Prepared(queryId, null, null)
      val preparedStatement = new PreparedStatement(ExPrepared(prepared, query, null))
      val future = session.execute(preparedStatement.bind())
      router.expectMsgPF(){ case Execute(id, _) if id == queryId => true }
      router.reply(NodeAwareError(Error(UNPREPARED, "msg"), node))
      router.expectMsg(Prepare(query))
      val newQueryId = new PreparedId("id2".getBytes)
      val newPrepared = new Prepared(newQueryId, null, null)
      router.reply(ExPrepared(newPrepared, query, node))
      router.expectMsg(PrepareOnAllNodes(query, node))
      router.expectMsgPF(){ case Execute(id, _) if id == newQueryId => true }
      router.reply(Result(Void))
      val result = Await.result(future, 1 second)
      result.size should be(0)
      router.expectNoMsg()
    }

    it("should close") {
      session.close()
      router.expectMsg(CloseRouter)
    }

    it("should prepare on all hosts after successful prepare") {
      val node = new InetSocketAddress("localhost", 1234)
      val query = "prepare_query2"
      val res = session.prepare(query)
      router.expectMsgPF(){ case Prepare("prepare_query2") => true }
      router.reply(ExPrepared(null, query, node))
      router.expectMsg(PrepareOnAllNodes(query, node))
      val result = Await.result(res, 1 second)
      result shouldBe a[PreparedStatement]
    }
  }

}
