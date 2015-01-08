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

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, TestKit}
import net.vangas.cassandra.VangasActorTestSupport

import scala.collection.mutable.ListBuffer

class RoundRobinConnectionPoolSpec extends TestKit(ActorSystem("RRPoolSystem")) with VangasActorTestSupport {

  val pool = new RoundRobinConnectionPool

  describe("RoundRobinConnectionPool") {
    it("should add connection") {
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      pool.addConnection(connection1.ref)
      pool.addConnection(connection2.ref)
      pool.connections should be(ListBuffer(connection1.ref, connection2.ref))
      pool.hasConnection should be(true)
    }

    it("should remove connection") {
      val connection1 = TestProbe()
      pool.connections += connection1.ref
      pool.removeConnection(connection1.ref)
      pool.hasConnection should be(false)
    }

    it("should get next connection") {
      val connections = Seq.fill(2)(TestProbe())

      connections.foreach(c => pool.addConnection(c.ref))

      pool.nextConnection should be(Some(connections(0).ref))
      pool.nextConnection should be(Some(connections(1).ref))
      pool.nextConnection should be(Some(connections(0).ref))

      pool.index should be(3)
      val moreConnections = Seq.fill(2)(TestProbe())
      moreConnections.foreach(c => pool.addConnection(c.ref))

      pool.nextConnection should be(Some(moreConnections(1).ref))
      pool.nextConnection should be(Some(connections(0).ref))
      pool.nextConnection should be(Some(connections(1).ref))
      pool.nextConnection should be(Some(moreConnections(0).ref))

      pool.index should be(7)
    }
  }

}
