/*
 * Copyright (C) 2015 Egemen Kalyoncu
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
import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.pattern.gracefulStop
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import net.vangas.cassandra.VangasTestHelpers._
import net.vangas.cassandra._
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.message.TopologyChangeType._
import net.vangas.cassandra.message.StatusChangeType._
import net.vangas.cassandra.message.{StatusChangeEvent, Prepare, Query, TopologyChangeEvent}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import org.scalatest.Inspectors._

class ConnectionPoolManagerSpec extends TestKit(ActorSystem("ConnectionPoolManagerActorSystem")) with VangasActorTestSupport {

  val config = Configuration(connectionsPerNode = 1)

  def newConnectionPoolManager(nodes: Seq[InetSocketAddress] = Seq.empty,
                               connectionFactory: (InetSocketAddress) => ActorRef = _ => null,
                               config: Configuration = config) =
    TestActorRef(new ConnectionPoolManager(1, "TEST_KS", nodes, config) with CPManagerComponents {
      override def createConnection(queryTimeOut: FiniteDuration,
                                    connectionTimeout: FiniteDuration,
                                    node: InetSocketAddress): Actor = {
        log.info("Creating new connection actor for node[{}]", node)
        new ForwardingActor(connectionFactory(node))
      }
    })

  describe("ConnectionPoolManager") {

    it("should create connection per hosts") {
      val node1 = node(1111)
      val connection1 = TestProbe()
      val connectionPools = newConnectionPoolManager(Seq(node1), _ => connection1.ref)
      connection1.expectMsg("Started")
      connectionPools ! PoisonPill
      connection1.expectMsg("Closed")
    }

    it("should add connections when they are ready") {
      val node1 = node(1111)
      val node2 = node(2222)
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      val connectionPools = newConnectionPoolManager()
      system.eventStream.publish(ConnectionReady(connection1.ref, node1))
      system.eventStream.publish(ConnectionReady(connection2.ref, node2))
      connection1.expectMsgPF(){ case Query("USE TEST_KS", _) => true }
      connection2.expectMsgPF(){ case Query("USE TEST_KS", _) => true }
      val pools = connectionPools.underlyingActor.pools
      pools(node1).connections should be(mutable.ListBuffer(connection1.ref))
      pools(node2).connections should be(mutable.ListBuffer(connection2.ref))
    }

    it("should remove connection when it is closed") {
      val node1 = node(1111)
      val connection1 = TestProbe()
      val connectionPools = newConnectionPoolManager()
      system.eventStream.publish(ConnectionReady(connection1.ref, node1))
      Await.result(gracefulStop(connection1.ref, 1 second), 1.5 seconds)
      connectionPools.underlyingActor.pools(node1).connections.size should be(0)
    }

    it("should get connection for node") {
      val node1 = node(1111)
      val connection1 = TestProbe()
      val connectionPools = newConnectionPoolManager()
      system.eventStream.publish(ConnectionReady(connection1.ref, node1))
      connectionPools ! GetConnectionFor(node1)
      expectMsg(ConnectionReceived(connection1.ref, node1))
    }

    it("should return no connection msg when host has no connection") {
      val node1 = node(1111)
      val node2 = node(2222)
      val connection1 = TestProbe()
      val connectionPools = newConnectionPoolManager()
      system.eventStream.publish(ConnectionReady(connection1.ref, node1))
      connectionPools ! GetConnectionFor(node2)
      expectMsg(NoConnectionFor(node2))
    }

    it("should prepare on all nodes except currently prepared one") {
      val node1 = node(1111)
      val node2 = node(2222)
      val node3 = node(3333)
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      val connection3 = TestProbe()
      Seq(connection1, connection2, connection3).foreach(_.ignoreMsg{ case msg: Query => true})
      val connectionPools = newConnectionPoolManager()
      system.eventStream.publish(ConnectionReady(connection1.ref, node1))
      system.eventStream.publish(ConnectionReady(connection2.ref, node2))
      system.eventStream.publish(ConnectionReady(connection3.ref, node3))
      connectionPools ! PrepareOnAllNodes(Prepare("query_prepare"), node2)
      connection1.expectMsg(Prepare("query_prepare"))
      connection3.expectMsg(Prepare("query_prepare"))
    }

    it("should restart children connections when ConnectionPools actor restarted") {
      val node1 = node(1111)
      val connection1 = TestProbe()
      val configForNPE = Configuration(connectionsPerNode = 1, queryConfig = null)
      newConnectionPoolManager(Seq(node1), Map(node1 -> connection1.ref), configForNPE)
      connection1.expectMsg("Started")
      system.eventStream.publish(ConnectionReady(connection1.ref, node1))
      connection1.expectMsg("Closed")
      //Restarted if child is not killed during parent restart or child is started when parent is restarted
      connection1.expectMsgPF(){case "Started" | "Restarted" => true}
      connection1.expectNoMsg()
    }

    it("should stash messages when there is no ready connection") {
      val node1 = node(1111)
      val connection1 = TestProbe()
      val connectionPools = newConnectionPoolManager()

      connectionPools ! GetConnectionFor(node1)
      expectNoMsg()

      system.eventStream.publish(ConnectionReady(connection1.ref, node1))

      expectMsg(ConnectionReceived(connection1.ref, node1))

      connection1.expectMsgPF(){ case Query("USE TEST_KS", _) => true }
      val pools = connectionPools.underlyingActor.pools
      pools(node1).connections should be(mutable.ListBuffer(connection1.ref))
    }

    it("should stop stashing after 2 messages") {
      //2 is configured in application.conf under test folder
      val node1 = node(1111)
      val connectionPools = newConnectionPoolManager()

      connectionPools ! GetConnectionFor(node1)
      connectionPools ! GetConnectionFor(node1)
      connectionPools ! GetConnectionFor(node1) //We will loose this message because of StashOverflowException

      expectMsg(NoConnectionFor(node1))
      expectMsg(NoConnectionFor(node1))

      val pools = connectionPools.underlyingActor.pools
      pools(node1).connections should be(mutable.ListBuffer.empty)
    }

    forAll(Seq(
      TopologyChangeEvent(REMOVED_NODE, node("127.0.0.1").getAddress),
      StatusChangeEvent(DOWN, node("127.0.0.1").getAddress)
    )) { removeEvent =>
      it(s"should remove node and connections for $removeEvent") {
        val node1 = node("127.0.0.1")
        val node2 = node("127.0.0.2")
        val counter = new AtomicInteger(0)
        val connection1 = TestProbe()
        val connection2 = TestProbe()
        val connection3 = TestProbe()
        val connectionFactory = (node: InetSocketAddress) => {
          if (node == node1) {
            if (counter.getAndIncrement % 2 == 0) connection1.ref else connection2.ref
          } else {
            connection3.ref
          }
        }
        watch(connection1.ref)
        watch(connection2.ref)
        watch(connection3.ref)
        val cpManager = newConnectionPoolManager(nodes = Seq(node1, node2),
          connectionFactory = connectionFactory,
          config = Configuration(connectionsPerNode = 2))
        connection1.expectMsg("Started")
        connection2.expectMsg("Started")
        connection3.expectMsg("Started")

        cpManager ! ConnectionReady(connection1.ref, node1)
        cpManager ! ConnectionReady(connection2.ref, node1)
        cpManager ! ConnectionReady(connection3.ref, node2)

        system.eventStream.publish(removeEvent)
        expectTerminated(connection1.ref)
        expectTerminated(connection2.ref)
        expectNoMsg()
        cpManager.underlyingActor.pools.size should be(1)
        cpManager.underlyingActor.pools(node2).connections.toSeq should be(Seq(connection3.ref))
      }
    }


    forAll(Seq(
      TopologyChangeEvent(NEW_NODE, node("127.0.0.1").getAddress),
      StatusChangeEvent(UP, node("127.0.0.1").getAddress)
      )) { addEvent =>
      it(s"should add new node and connections for $addEvent") {
        val node1 = node("127.0.0.1")
        var counter = -1
        val connection1 = TestProbe()
        val connection2 = TestProbe()
        val connectionFactory = (node: InetSocketAddress) => {
          counter += 1
          if (counter % 2 == 0) connection1.ref else connection2.ref
        }
        watch(connection1.ref)
        watch(connection2.ref)
        val cpManager = newConnectionPoolManager(connectionFactory = connectionFactory)

        system.eventStream.publish(addEvent)
        connection1.expectMsg("Started")
        cpManager ! ConnectionReady(connection1.ref, node1)
        connection1.expectMsgType[Query]

        system.eventStream.publish(addEvent)
        expectTerminated(connection1.ref)
        connection2.expectMsg("Started")
      }
    }
  }

}
