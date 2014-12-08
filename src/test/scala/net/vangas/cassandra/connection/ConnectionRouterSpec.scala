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

import akka.actor._
import akka.io.Tcp.Connect
import akka.pattern.gracefulStop
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import net.vangas.cassandra.{ConnectionReady, _}
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.message.{Prepare, Query}
import org.mockito.Mockito.{times, verify, when}
import org.mockito.Matchers.any
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, OneInstancePerTest}

import scala.concurrent.Await
import scala.concurrent.duration._

class ConnectionRouterSpec extends TestKit(ActorSystem("ConnectionRouterSystem"))
  with FunSpecLike with ImplicitSender with BeforeAndAfterAll with OneInstancePerTest {

  val connectionPool = mock[ConnectionPool]

  private def createConnectionRouter(connection: ActorRef = TestProbe().ref): TestActorRef[ConnectionRouter] =
    TestActorRef(
      new ConnectionRouter("TEST_KS", Seq(new InetSocketAddress("localhost", 9999)), Configuration(), connectionPool) with ConnectionRouterComponents {
        override def createConnection(queryTimeout: FiniteDuration,
                                      connectionTimeout: FiniteDuration,
                                      node: InetSocketAddress): Actor = new ForwardingActor(connection)
      }
    )

  override def afterAll() {
    system.shutdown()
  }

  describe("ConnectionRouter") {
    it("should get ConnectionReady message") {
      val nodeAddress = new InetSocketAddress("localhost", 1234)
      val connection = TestProbe()
      createConnectionRouter()
      system.eventStream.publish(ConnectionReady(connection.ref, nodeAddress))
      verify(connectionPool).addConnection(connection.ref, nodeAddress)
      connection.expectMsgPF(){ case Query("USE TEST_KS", _) => true }
    }

    it("should remove connection and unbecome when it dies") {
      val connection = TestProbe()
      val connectionRouter = createConnectionRouter()
      connectionRouter.watch(connection.ref)
      Await.result(gracefulStop(connection.ref, 1 second), 1.5 seconds)
      verify(connectionPool).removeConnection(connection.ref)
      when(connectionPool.hasNoConnection).thenReturn(true)
      connectionRouter.underlyingActor.isReady should be(false)
    }

    it("should send message to connection") {
      val connection = TestProbe()
      when(connectionPool.next).thenReturn(connection.ref)
      val router = createConnectionRouter()
      router.underlying.become(router.underlyingActor.ready)
      router ! Query("query", null)
      verify(connectionPool).next
      connection.expectMsg(Query("query", null))
    }

    it("should unstash all stashed messages when at least one connection is ready") {
      val nodeAddress = new InetSocketAddress("localhost", 1234)
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      val router = createConnectionRouter()
      router ! Query("query1", null)
      router ! Query("query2", null)

      when(connectionPool.next).thenReturn(connection1.ref)
      system.eventStream.publish(ConnectionReady(connection1.ref, nodeAddress))

      when(connectionPool.next).thenReturn(connection2.ref)
      system.eventStream.publish(ConnectionReady(connection2.ref, nodeAddress))

      connection1.expectMsgPF(){ case Query("USE TEST_KS", _) => true }
      connection2.expectMsgPF(){ case Query("USE TEST_KS", _) => true }

      verify(connectionPool).addConnection(connection1.ref, nodeAddress)
      verify(connectionPool).addConnection(connection2.ref, nodeAddress)
      verify(connectionPool, times(2)).next
      connection1.expectMsg(Query("query1", null))
      connection1.expectMsg(Query("query2", null))
      connection1.expectNoMsg()
      connection2.expectNoMsg()
    }

    it("should stash messages when there is no ready connection") {
      val nodeAddress = new InetSocketAddress("localhost", 1234)
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      val connectionRouter = createConnectionRouter()
      system.eventStream.publish(ConnectionReady(connection1.ref, nodeAddress)) //To watch connection1
      connection1.expectMsgPF(){ case Query("USE TEST_KS", _) => true }

      when(connectionPool.hasNoConnection).thenReturn(true)
      Await.result(gracefulStop(connection1.ref, 1 second), 1.5 seconds)

      connectionRouter ! Query("query3", null)
      connection1.expectNoMsg() //because msgs are stashed

      when(connectionPool.next).thenReturn(connection2.ref)
      system.eventStream.publish(ConnectionReady(connection2.ref, nodeAddress))

      connection2.expectMsgPF(){ case Query("USE TEST_KS", _) => true }
      connection2.expectMsg(Query("query3", null))
    }

    it("should retry request when max streamId is reached") {
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      val router = createConnectionRouter()
      when(connectionPool.next).thenReturn(connection2.ref)
      router.underlying.become(router.underlyingActor.ready)
      system.eventStream.publish(MaxStreamIdReached(Query("retry_query", null), self, connection1.ref, 3))
      verify(connectionPool).next
      connection2.expectMsg(RetryFailedRequest(Query("retry_query", null), 2))
      connection1.expectNoMsg()
    }

    it("should NOT retry request when numOfRetries is ZERO") {
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      when(connectionPool.next).thenReturn(connection2.ref)
      val router = createConnectionRouter()
      router.underlying.become(router.underlyingActor.ready)
      system.eventStream.publish(MaxStreamIdReached(Query("retry_query", null), self, connection1.ref, 0))
      verify(connectionPool, times(0)).next
      connection1.expectNoMsg()
      connection2.expectNoMsg()
    }

    it("should restart children after ConnectionRouter RESTART") {
      val mockIoManager = TestProbe()
      val node = new InetSocketAddress("localhost", 1234)
      val realConnectionRouter =
        system.actorOf(Props(new ConnectionRouter("TEST_KS", Seq(node), Configuration(), connectionPool) with ConnectionRouterComponents {
          override def createConnection(queryTimeout: FiniteDuration,connectionTimeout: FiniteDuration, node: InetSocketAddress): Actor =
            new Connection(connectionTimeout, node, null, null) with ConnectionComponents {
              override def createResponseHandlerActor: Actor = new ForwardingActor(null)
              override def ioManager(implicit system: ActorSystem): ActorRef = mockIoManager.ref
            }
        }))
      mockIoManager.expectMsg(Connect(node, timeout = Some(Configuration().connectionTimeout seconds)))
      when(connectionPool.next).thenThrow(new RuntimeException("~~TEST~~"))
      system.eventStream.publish(ConnectionReady(self, node))
      realConnectionRouter ! Query("query1", null)
      mockIoManager.expectMsg(Connect(node, timeout = Some(Configuration().connectionTimeout seconds)))
    }

    it("should close itself and child connections") {
      val connection = TestProbe()
      val connectionRouter = createConnectionRouter(connection.ref)
      connectionRouter ! CloseRouter
      connection.expectMsg("Closed")
    }

    it("should send prepare request to all hosts except prepared one") {
      val connection = TestProbe()
      val node = new InetSocketAddress("localhost", 1234)
      when(connectionPool.executeOnAllNodes(any())).thenAnswer(new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit = {
          val func = invocation.getArguments.head.asInstanceOf[ConnectionPool#Entity => Unit]
          func((node, connection.ref))
        }
      })
      val router = createConnectionRouter()
      router.underlying.become(router.underlyingActor.ready)
      router ! PrepareOnAllNodes("query", new InetSocketAddress("localhost", 1235))
      connection.expectMsg(Prepare("query"))

      router ! PrepareOnAllNodes("query", new InetSocketAddress("localhost", 1234))
      connection.expectNoMsg()
    }

  }

}
