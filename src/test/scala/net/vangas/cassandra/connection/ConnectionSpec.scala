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

import org.joda.time.{DateTimeUtils, DateTime}
import org.scalatest.{BeforeAndAfter, OneInstancePerTest, BeforeAndAfterAll, FunSpecLike}
import org.scalatest.mock.MockitoSugar._
import org.scalatest.Matchers._
import org.mockito.Mockito.when
import org.mockito.Mockito.verify
import akka.testkit.{TestProbe, TestActorRef, ImplicitSender, TestKit}
import akka.actor.{PoisonPill, ActorRef, Actor, ActorSystem}
import akka.io.Tcp._
import akka.util.ByteString
import net.vangas.cassandra.util.FrameUtils._
import net.vangas.cassandra.message._
import net.vangas.cassandra._
import scala.concurrent.duration._
import akka.io.Tcp.Connected
import net.vangas.cassandra.RequestStream
import net.vangas.cassandra.message.Execute
import akka.io.Tcp.Register
import akka.io.Tcp.Connect
import akka.io.Tcp.Received
import net.vangas.cassandra.message.Prepare
import net.vangas.cassandra.message.Query

class ConnectionSpec extends TestKit(ActorSystem("ConnectionSpecSystem"))
  with FunSpecLike with ImplicitSender with BeforeAndAfter with BeforeAndAfterAll with OneInstancePerTest {

  val responseHandlerProbe = TestProbe()
  val ioProbe = TestProbe()
  val streamContext = mock[StreamContext]
  val streamIdExtractor = mock[StreamIdExtractor]
  val connectionTimeout = 1 second
  val fixedTime = DateTime.now

  val connectionActor = TestActorRef(new Connection(connectionTimeout, null, streamContext, streamIdExtractor) with MockConnectionComponents)

  override def afterAll() {
    system.shutdown()
  }

  before {
    DateTimeUtils.setCurrentMillisFixed(fixedTime.getMillis)
  }

  after {
    DateTimeUtils.setCurrentMillisSystem()
  }

  describe("Connection") {
    it("should be ready for queries") {
      val eventProbe = TestProbe()
      system.eventStream.subscribe(eventProbe.ref, classOf[ConnectionReady])
      ioProbe.expectMsg(Connect(null, timeout = Some(connectionTimeout)))
      makeConnected
      connectionActor.underlyingActor.cassandraConnection = self
      connectionActor.underlyingActor.isReady should be(false)
      connectionActor ! Ready
      connectionActor ! IsConnectionReady
      connectionActor.underlyingActor.isReady should be(true)
      expectMsg(true)
      eventProbe.expectMsg(ConnectionReady(connectionActor, null))
    }

    it("should send startup request") {
      val id = 99.toShort
      when(streamContext.registerStream(RequestStream(connectionActor, Startup))).thenReturn(Some(id))
      connectionActor ! Connected(null, null)
      expectMsg(Register(connectionActor))
      expectMsg(Write(serializeFrame(id, Startup)))
      connectionActor.underlyingActor.cassandraConnection = self
      responseHandlerProbe.expectMsg(Connected(null, null))
      expectNoMsg()
    }

    it("should redirect server responses to the ResponseHandler actor") {
      val res = ByteString.fromString("TEST_RESPONSE")
      makeConnected
      when(streamIdExtractor.streamId(res)).thenReturn(0.toShort)
      when(streamContext.getRequester(0)).thenReturn(Some(RequestStream(self, Query("q", null))))
      connectionActor ! Received(res)
      responseHandlerProbe.expectMsg(ReceivedData(res, RequestStream(self, Query("q", null))))
      verify(streamContext).releaseStream(0.toShort)
    }

    it("should not redirect server responses for unknown stream") {
      val res = ByteString.fromString("TEST_RESPONSE")
      makeConnected
      val unKnowStreamId: Short = 2
      when(streamIdExtractor.streamId(res)).thenReturn(unKnowStreamId)
      connectionActor ! Received(res)
      responseHandlerProbe.expectNoMsg()
    }

    it("should query server") {
      makeConnected
      val cassandraConnection = TestProbe()
      val query = Query("query_string", new QueryParameters(ConsistencyLevel.ONE, Seq("a"), false, 2))
      connectionActor.underlyingActor.cassandraConnection = cassandraConnection.ref
      when(streamContext.registerStream(RequestStream(self, query))).thenReturn(Some(0.toShort))
      connectionActor ! query
      verify(streamContext).registerStream(RequestStream(self, query))
      cassandraConnection.expectMsg(Write(serializeFrame(0, query)))
    }

    it("should prepare query") {
      makeConnected
      val cassandraConnection = TestProbe()
      val prepare = Prepare("query_string")
      connectionActor.underlyingActor.cassandraConnection = cassandraConnection.ref
      when(streamContext.registerStream(RequestStream(self, prepare))).thenReturn(Some(0.toShort))
      connectionActor ! prepare
      verify(streamContext).registerStream(RequestStream(self, prepare))
      cassandraConnection.expectMsg(Write(serializeFrame(0, prepare)))
    }

    it("should execute prepared query") {
      makeConnected
      val cassandraConnection = TestProbe()
      connectionActor.underlyingActor.cassandraConnection = cassandraConnection.ref
      val queryParameters = new QueryParameters(ConsistencyLevel.QUORUM, Seq.empty, false, -1, None, ConsistencyLevel.SERIAL)
      val execute = Execute(new PreparedId("ID".getBytes), queryParameters)
      when(streamContext.registerStream(RequestStream(self, execute))).thenReturn(Some(0.toShort))
      connectionActor ! execute
      cassandraConnection.expectMsg(Write(serializeFrame(0, execute)))
    }

    it("should close connection when actor is died") {
      val connectionActor = TestActorRef(new Connection(connectionTimeout, null, streamContext, streamIdExtractor) with MockConnectionComponents)
      connectionActor.underlyingActor.cassandraConnection = self
      connectionActor ! PoisonPill
      expectMsg(Close)
    }

    it("should send RetryMessage on MaxConcurrentRequestException") {
      makeConnected
      system.eventStream.subscribe(self, classOf[MaxStreamIdReached])
      when(streamContext.registerStream(RequestStream(self, Query("QUERY_TO_RETRY", null)))).thenReturn(None)
      connectionActor ! Query("QUERY_TO_RETRY", null)
      expectMsg(MaxStreamIdReached(Query("QUERY_TO_RETRY", null), self, connectionActor, 3))
      system.eventStream.unsubscribe(self, classOf[RetryFailedRequest])
    }

    it("should close connection") {
      makeConnected
      connectionActor ! CloseConnection
      expectMsg(Close)
    }

    it("should clean expired streams") {
      makeConnected
      system.eventStream.subscribe(self, classOf[MaxStreamIdReached])
      when(streamContext.registerStream(RequestStream(self, Query("test", null)))).thenReturn(None)
      connectionActor ! Query("test", null)
      expectMsgType[MaxStreamIdReached]
      verify(streamContext).cleanExpiredStreams()
      system.eventStream.unsubscribe(self, classOf[RetryFailedRequest])
    }
  }

  trait MockConnectionComponents extends ConnectionComponents {
    override def createResponseHandlerActor: Actor = new ForwardingActor(responseHandlerProbe.ref)
    override def ioManager(implicit system: ActorSystem): ActorRef = ioProbe.ref
  }

  private def makeConnected {
    connectionActor.underlying.become(connectionActor.underlyingActor.connected)
    connectionActor.underlyingActor.cassandraConnection = self
  }

}
