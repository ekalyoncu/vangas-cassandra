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

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import org.scalatest.{OneInstancePerTest, BeforeAndAfterAll, FunSpecLike}
import org.scalatest.Matchers._
import scala.collection.mutable

class DefaultConnectionPoolSpec extends TestKit(ActorSystem("DefaultConnectionPoolSpecSystem"))
with FunSpecLike with ImplicitSender with BeforeAndAfterAll with OneInstancePerTest {

  val pool = new DefaultConnectionPool

  override def afterAll() {
    system.shutdown()
  }

  describe("DefaultConnectionPool") {
    it("should add connection") {
      val node = new InetSocketAddress("localhost", 1234)
      val connection = TestProbe()
      pool.addConnection(connection.ref, node)
      pool.connections should be (mutable.Buffer(connection.ref))
      pool.connectionsPerNode should be (mutable.Map(node -> mutable.Buffer(connection.ref)))
      pool.addConnection(connection.ref, node)
      pool.connections should be (mutable.Buffer(connection.ref))
      pool.connectionsPerNode should be (mutable.Map(node -> mutable.Buffer(connection.ref)))
    }

    it("should return for size, hasConnection and hasNoConnection") {
      val node = new InetSocketAddress("localhost", 1234)
      val connection = TestProbe()
      pool.hasConnection should be(false)
      pool.hasNoConnection should be(true)
      pool.addConnection(connection.ref, node)
      pool.hasConnection should be(true)
      pool.hasNoConnection should be(false)
    }

    it("should remove connection") {
      val node = new InetSocketAddress("localhost", 1234)
      val connection = TestProbe()
      pool.addConnection(connection.ref, node)
      pool.removeConnection(connection.ref)
      pool.hasConnection should be(false)
      pool.hasNoConnection should be(true)
      pool.connections should be (mutable.Buffer.empty)
      pool.connectionsPerNode should be (mutable.Map(node -> mutable.Buffer.empty))
    }

    it("should get next connection") {
      val node = new InetSocketAddress("localhost", 1234)
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      pool.addConnection(connection1.ref, node)
      pool.addConnection(connection2.ref, node)
      pool.next should be(connection1.ref)
      pool.next should be(connection2.ref)
      pool.next should be(connection1.ref)
    }

    it("should executeOnAllNodes(using only one connection per node)") {
      val node1 = new InetSocketAddress("localhost", 1111)
      val connection1ToNode1 = TestProbe()
      val connection2ToNode1 = TestProbe()
      pool.addConnection(connection1ToNode1.ref, node1)
      pool.addConnection(connection2ToNode1.ref, node1)

      val node2 = new InetSocketAddress("localhost", 2222)
      val connection1ToNode2 = TestProbe()
      val connection2ToNode2 = TestProbe()
      pool.addConnection(connection1ToNode2.ref, node2)
      pool.addConnection(connection2ToNode2.ref, node2)

      pool.executeOnAllNodes{ case(node, connection) =>
        connection ! ("test", node)
      }

      connection1ToNode1.expectMsg(("test", node1))
      connection1ToNode2.expectMsg(("test", node2))
      connection2ToNode1.expectNoMsg()
      connection2ToNode2.expectNoMsg()
    }

  }

}
