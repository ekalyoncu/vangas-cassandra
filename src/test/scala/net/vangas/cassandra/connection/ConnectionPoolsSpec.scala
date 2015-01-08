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

import akka.actor._
import akka.pattern.gracefulStop
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import net.vangas.cassandra._
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.message.{Prepare, Query}
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import VangasTestHelpers._

class ConnectionPoolsSpec extends TestKit(ActorSystem("ConnectionPoolsActorSystem")) with VangasActorTestSupport {

  val config = Configuration(connectionsPerNode = 1)

  def newConnectionPools(nodes: Seq[InetSocketAddress] = Seq.empty,
                         connections: Map[InetSocketAddress, ActorRef] = Map.empty,
                         config: Configuration = config) =
    TestActorRef(new ConnectionPools("TEST_KS", nodes, config) with ConnectionPoolsComponents {
      override def createConnection(queryTimeOut: FiniteDuration,
                                    connectionTimeout: FiniteDuration,
                                    node: InetSocketAddress): Actor = new ForwardingActor(connections(node))
    })

  describe("ConnectionPools") {

    it("should create connection per hosts") {
      val node1 = node(1111)
      val connection1 = TestProbe()
      val connectionPools = newConnectionPools(Seq(node1), Map(node1 -> connection1.ref))
      connection1.expectMsg("Started")
      connectionPools ! PoisonPill
      connection1.expectMsg("Closed")
    }

    it("should add connections when they are ready") {
      val node1 = node(1111)
      val node2 = node(2222)
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      val connectionPools = newConnectionPools()
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
      val connectionPools = newConnectionPools()
      system.eventStream.publish(ConnectionReady(connection1.ref, node1))
      Await.result(gracefulStop(connection1.ref, 1 second), 1.5 seconds)
      connectionPools.underlyingActor.pools(node1).connections.size should be(0)
    }

    it("should get connection for node") {
      val node1 = node(1111)
      val connection1 = TestProbe()
      val connectionPools = newConnectionPools()
      system.eventStream.publish(ConnectionReady(connection1.ref, node1))
      connectionPools ! GetConnectionFor(node1)
      expectMsg(ConnectionReceived(connection1.ref))
    }

    it("should return no connection msg when host has no connection") {
      val node1 = node(1111)
      val node2 = node(2222)
      val connection1 = TestProbe()
      val connectionPools = newConnectionPools()
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
      val connectionPools = newConnectionPools()
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
      newConnectionPools(Seq(node1), Map(node1 -> connection1.ref), configForNPE)
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
      val connectionPools = newConnectionPools()

      connectionPools ! GetConnectionFor(node1)
      expectNoMsg()

      system.eventStream.publish(ConnectionReady(connection1.ref, node1))

      expectMsg(ConnectionReceived(connection1.ref))

      connection1.expectMsgPF(){ case Query("USE TEST_KS", _) => true }
      val pools = connectionPools.underlyingActor.pools
      pools(node1).connections should be(mutable.ListBuffer(connection1.ref))
    }

    it("should stop stashing after 2 messages") {
      //2 is configured in application.conf under test folder
      val node1 = node(1111)
      val connectionPools = newConnectionPools()

      connectionPools ! GetConnectionFor(node1)
      connectionPools ! GetConnectionFor(node1)
      connectionPools ! GetConnectionFor(node1) //We will loose this message because of StashOverflowException

      expectMsg(NoConnectionFor(node1))
      expectMsg(NoConnectionFor(node1))

      val pools = connectionPools.underlyingActor.pools
      pools(node1).connections should be(mutable.ListBuffer.empty)
    }

  }

}
