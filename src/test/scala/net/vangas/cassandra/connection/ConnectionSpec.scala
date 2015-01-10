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

import org.joda.time.{DateTimeUtils, DateTime}
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.when
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions
import org.mockito.Mockito.verifyZeroInteractions
import akka.testkit.{TestProbe, TestActorRef, TestKit}
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
import VangasTestHelpers._

class ConnectionSpec extends TestKit(ActorSystem("ConnectionSpecSystem"))
  with VangasActorTestSupport with BeforeAndAfter {

  val node1 = node(111)
  val responseHandlerProbe = TestProbe()
  val ioProbe = TestProbe()
  val streamContext = mock[StreamContext]
  val streamIdExtractor = mock[StreamIdExtractor]
  val connectionTimeout = 1 second
  val fixedTime = DateTime.now

  val connectionActor = TestActorRef(new Connection(connectionTimeout, node1, streamContext, streamIdExtractor) with MockConnectionComponents)

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
      ioProbe.expectMsg(Connect(node1, timeout = Some(connectionTimeout)))
      makeConnected
      connectionActor.underlyingActor.cassandraConnection = self
      connectionActor ! Ready
      eventProbe.expectMsg(ConnectionReady(connectionActor, node1))
    }

    it("should send startup request") {
      responseHandlerProbe.expectMsg("Started")
      val id = 99.toShort
      when(streamContext.registerStream(RequestStream(connectionActor, Startup))).thenReturn(Some(id))
      connectionActor ! Connected(null, null)
      expectMsg(Register(connectionActor))
      expectMsg(Write(serializeFrame(id, Startup)))
      connectionActor.underlyingActor.cassandraConnection = self
      responseHandlerProbe.expectNoMsg()
      expectNoMsg()
    }

    it("should redirect server responses to the ResponseHandler actor") {
      responseHandlerProbe.expectMsg("Started")
      val res = ByteString.fromString("TEST_RESPONSE")
      makeConnected
      when(streamIdExtractor.streamId(res)).thenReturn(0.toShort)
      when(streamContext.getRequester(0)).thenReturn(Some(RequestStream(self, Query("q", null))))
      connectionActor ! Received(res)
      responseHandlerProbe.expectMsg(ReceivedData(res, RequestStream(self, Query("q", null))))
      verify(streamContext).releaseStream(0.toShort)
    }

    it("should redirect server-side events to response handler") {
      responseHandlerProbe.expectMsg("Started")
      makeConnected
      val event = ByteString.fromString("SERVER_SIDE_EVENT")
      when(streamIdExtractor.streamId(event)).thenReturn(-1.toShort)
      connectionActor ! Received(event)
      responseHandlerProbe.expectMsg(event)
      verifyZeroInteractions(streamContext)
    }

    it("should not redirect server responses for unknown stream") {
      responseHandlerProbe.expectMsg("Started")
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

    it("should send MaxStreamIdReached msg when connection has no slot for new stream") {
      makeConnected
      when(streamContext.registerStream(RequestStream(self, Query("QUERY_TO_RETRY", null)))).thenReturn(None)
      connectionActor ! Query("QUERY_TO_RETRY", null)
      expectMsg(MaxStreamIdReached(connectionActor))
    }

    it("should clean expired streams") {
      makeConnected
      when(streamContext.registerStream(RequestStream(self, Query("test", null)))).thenReturn(None)
      connectionActor ! Query("test", null)
      expectMsgType[MaxStreamIdReached]
      verify(streamContext).cleanExpiredStreams()
    }

    it("should restart connection when there it gets CommandFailure") {
      ioProbe.expectMsg(Connect(node1, timeout = Some(connectionTimeout)))
      connectionActor.underlyingActor.context.become(connectionActor.underlyingActor.connected)
      system.eventStream.subscribe(self, classOf[ConnectionDefunct])
      connectionActor ! CommandFailed(Write(ByteString.fromString("~TEST~")))
      expectMsg(ConnectionDefunct(connectionActor, node1))
      ioProbe.expectMsg(Connect(node1, timeout = Some(connectionTimeout)))
    }

    it("should register for events") {
      makeConnected
      val register = RegisterForEvents(Seq("TOPOLOGY_CHANGE", "STATUS_CHANGE"))
      when(streamContext.registerStream(RequestStream(null, null))).thenReturn(Some(3.toShort))
      connectionActor ! register
      expectMsg(Write(serializeFrame(3, register)))
      verify(streamContext).registerStream(RequestStream(null, null))
      verify(streamContext).releaseStream(3.toShort)
      verifyNoMoreInteractions(streamContext)
    }
  }

  trait MockConnectionComponents extends ConnectionComponents {
    override def createResponseHandlerActor(node: InetSocketAddress): Actor = new ForwardingActor(responseHandlerProbe.ref)
    override def ioManager(implicit system: ActorSystem): ActorRef = ioProbe.ref
  }

  private def makeConnected {
    connectionActor.underlying.become(connectionActor.underlyingActor.connected)
    connectionActor.underlyingActor.cassandraConnection = self
  }

}
