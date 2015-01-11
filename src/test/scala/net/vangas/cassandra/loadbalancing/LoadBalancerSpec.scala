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

package net.vangas.cassandra.loadbalancing

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit}
import net.vangas.cassandra._
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.message.{StatusChangeEvent, TopologyChangeEvent}
import net.vangas.cassandra.message.StatusChangeType._
import net.vangas.cassandra.message.TopologyChangeType._
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.when
import org.mockito.Mockito.verify
import scala.concurrent.duration._
import VangasTestHelpers._

class LoadBalancerSpec extends TestKit(ActorSystem("LoadBalancerSystem"))
  with VangasActorTestSupport {

  val lbPolicy = mock[LoadBalancingPolicy]
  val lb = TestActorRef(new LoadBalancer(lbPolicy, Configuration()))

  describe("LoadBalancer") {
    it("should return queryplan") {
      when(lbPolicy.newQueryPlan).thenReturn(QueryPlan(Iterator(node(1111), node(2222))))
      lb ! CreateQueryPlan
      val queryPlan = receiveOne(100 milliseconds).asInstanceOf[QueryPlan]
      queryPlan.nodes.toSeq should be(Seq(node(1111), node(2222)))
      verify(lbPolicy).newQueryPlan
    }

    it("should update loadbalancerpolicy when host is added") {
      lb ! TopologyChangeEvent(NEW_NODE, InetAddress.getByName("localhost"))
      verify(lbPolicy).onNodeAdded(node(9042))
    }

    it("should update loadbalancerpolicy when host is removed") {
      lb ! TopologyChangeEvent(REMOVED_NODE, InetAddress.getByName("localhost"))
      verify(lbPolicy).onNodeRemoved(node(9042))
    }

    it("should update loadbalancerpolicy when host is up") {
      lb ! StatusChangeEvent(UP, InetAddress.getByName("localhost"))
      verify(lbPolicy).onNodeUp(node(9042))
    }

    it("should update loadbalancerpolicy when host is down") {
      lb ! StatusChangeEvent(DOWN, InetAddress.getByName("localhost"))
      verify(lbPolicy).onNodeDown(node(9042))
    }
  }
}
