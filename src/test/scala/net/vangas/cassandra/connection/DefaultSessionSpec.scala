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

import java.util.concurrent.TimeoutException

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.{TestKit, TestProbe}
import net.vangas.cassandra.VangasTestHelpers._
import net.vangas.cassandra._
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.error.{RequestError, RequestErrorCode}
import net.vangas.cassandra.exception.{QueryExecutionException, QueryPrepareException}
import net.vangas.cassandra.message._
import org.scalatest.BeforeAndAfter

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DefaultSessionSpec extends TestKit(ActorSystem("DefaultSessionSystem")) with VangasActorTestSupport with BeforeAndAfter {

  val requestLifeCycle = TestProbe()
  val loadBalancer = TestProbe()
  val connectionPoolManager = TestProbe()
  val session = new DefaultSession(1, "SESSION_KS", Configuration(), loadBalancer.ref, connectionPoolManager.ref) with MockSessionComponents

  before {
    requestLifeCycle.expectMsg("Started")
//    when(sessionActorBridge.createRequestLifecycle()).thenReturn(requestLifeCycle.ref)
  }

  describe("DefaultSession") {
    it("should execute simple query and return resultset") {
      val future = session.execute("simple_query")
      requestLifeCycle.expectMsg(SimpleStatement("simple_query"))
      requestLifeCycle.reply(new EmptyResultSet)

      val result = Await.result(future, 1 second)
      result.size should be(0)
//      verify(sessionActorBridge).createRequestLifecycle()
    }

    it("should execute simple query and return error") {
      val future = session.execute("simple_query")
      requestLifeCycle.expectMsg(SimpleStatement("simple_query"))
      requestLifeCycle.reply(RequestError(RequestErrorCode.SYNTAX_ERROR, "~TEST~"))

      intercept[QueryExecutionException] {
        Await.result(future, 1 second)
      }
//      verify(sessionActorBridge).createRequestLifecycle()
    }

    it("should throw exception for USE statement") {
      val ex1 = intercept[IllegalArgumentException] {
        Await.result(session.execute("USE new_ks"), 1 second)
      }
      val ex2 = intercept[IllegalArgumentException] {
        Await.result(session.execute(SimpleStatement("USE new_ks")), 1 second)
      }
      ex1.getMessage should be(ex2.getMessage)
      ex1.getMessage should be("USE statement is not supported in session. Please create new session to query different keyspace")
    }

    it("should throw timeoutexception when server doesn't return in time") {
//      val newSession = new DefaultSession(1, "SESSION_KS", Configuration(queryTimeout = 100 milliseconds), null, null)
//      intercept[TimeoutException] {
//        Await.result(newSession.execute("timed out query"), 300 milliseconds)
//      }
    }

    it("should prepare statement") {
      val prepareQuery = "prepare_query"
      val future = session.prepare(prepareQuery)
      requestLifeCycle.expectMsg(PrepareStatement(prepareQuery))
      val prepared = Prepared(new PreparedId("ID".getBytes), null, null)
      requestLifeCycle.reply(ExPrepared(prepared, prepareQuery, node(1111)))

      val result = Await.result(future, 1 second)
      result.prepared() should be(ExPrepared(prepared, prepareQuery, node(1111)))
//      verify(sessionActorBridge).createRequestLifecycle()
    }

    it("should throw QueryPrepareException") {
      val future = session.prepare("prepare_query")
      requestLifeCycle.expectMsg(PrepareStatement("prepare_query"))
      requestLifeCycle.reply(RequestError(RequestErrorCode.SYNTAX_ERROR, "~TEST~"))

      intercept[QueryPrepareException] {
        Await.result(future, 1 second)
      }
//      verify(sessionActorBridge).createRequestLifecycle()
    }

    it("should close") {
      session.close()
//      verify(sessionActorBridge).closeSession()

      intercept[IllegalStateException] {
        Await.result(session.execute("query"), 1 second)
      }
      intercept[IllegalStateException] {
        Await.result(session.prepare("query"), 1 second)
      }
    }
  }

  trait MockSessionComponents extends SessionComponents {
    override def createRequestLifecycle(loadBalancer: ActorRef, connectionPoolManager: ActorRef, config: Configuration): Actor = {
      new ForwardingActor(requestLifeCycle.ref)
    }
  }

}
