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
import akka.actor.{Terminated, ActorSystem}
import akka.testkit.{TestKit, TestProbe}
import net.vangas.cassandra.VangasTestHelpers._
import net.vangas.cassandra._
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.error.{RequestError, RequestErrorCode}
import net.vangas.cassandra.exception.{QueryExecutionException, QueryPrepareException}
import net.vangas.cassandra.message._
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.when
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyZeroInteractions

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DefaultSessionSpec extends TestKit(ActorSystem("DefaultSessionSystem")) with VangasActorTestSupport with BeforeAndAfter {

  val sessionActor = TestProbe()
  val actorBridge = mock[DriverActorBridge]
  val session = new DefaultSession(1, sessionActor.ref, actorBridge, Configuration())


  describe("DefaultSession") {
    it("should execute simple query and return resultset") {
      val future = session.execute("simple_query")
      sessionActor.expectMsg(SimpleStatement("simple_query"))
      sessionActor.reply(Right(new EmptyResultSet))

      val result = Await.result(future, 1 second)
      result.size should be(0)
      verify(actorBridge).isSessionClosed(1)
    }

    it("should return failed future when actorBridge says session actor is dead") {
      when(actorBridge.isSessionClosed(1)).thenReturn(true)
      intercept[IllegalStateException] {
        Await.result(session.execute("simple_query"), 100 milliseconds)
      }
      verify(actorBridge).isSessionClosed(1)
    }

    it("should execute simple query and return error") {
      val future = session.execute("simple_query")
      sessionActor.expectMsg(SimpleStatement("simple_query"))
      sessionActor.reply(Left(RequestError(RequestErrorCode.SYNTAX_ERROR, "~TEST~")))

      intercept[QueryExecutionException] {
        Await.result(future, 100 milliseconds)
      }
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
      val newSession = new DefaultSession(1, sessionActor.ref, actorBridge, Configuration(queryTimeout = 100 milliseconds))
      intercept[TimeoutException] {
        Await.result(newSession.execute("timed out query"), 300 milliseconds)
      }
    }

    it("should prepare statement") {
      val prepareQuery = "prepare_query"
      val future = session.prepare(prepareQuery)
      sessionActor.expectMsg(PrepareStatement(prepareQuery))
      val prepared = Prepared(new PreparedId("ID".getBytes), null, null)
      sessionActor.reply(Right(ExPrepared(prepared, prepareQuery, node(1111))))

      val result = Await.result(future, 1 second)
      result.prepared() should be(ExPrepared(prepared, prepareQuery, node(1111)))
      verify(actorBridge).isSessionClosed(1)
    }

    it("should throw QueryPrepareException") {
      val future = session.prepare("prepare_query")
      sessionActor.expectMsg(PrepareStatement("prepare_query"))
      sessionActor.reply(Left(RequestError(RequestErrorCode.SYNTAX_ERROR, "~TEST~")))

      intercept[QueryPrepareException] {
        Await.result(future, 1 second)
      }
    }

    it("should close session") {
      watch(sessionActor.ref)
      Await.result(session.close(), 100 milliseconds) should be(true)
      receiveOne(100 milliseconds).asInstanceOf[Terminated].actor should be(sessionActor.ref)
      intercept[IllegalStateException] {
        Await.result(session.execute("simple_query"), 100 milliseconds)
      }
      intercept[IllegalStateException] {
        Await.result(session.prepare("prepare_query"), 100 milliseconds)
      }
      verifyZeroInteractions(actorBridge)
    }
  }
}
