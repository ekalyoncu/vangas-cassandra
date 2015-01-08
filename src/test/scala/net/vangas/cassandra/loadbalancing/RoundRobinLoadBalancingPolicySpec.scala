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

import org.scalatest.{OneInstancePerTest, FunSpec}
import org.scalatest.Matchers._
import net.vangas.cassandra.VangasTestHelpers._

class RoundRobinLoadBalancingPolicySpec extends FunSpec with OneInstancePerTest {

  val policy = new RoundRobinLoadBalancingPolicy
  policy.init(Seq(node(1111), node(2222)))

  describe("RoundRobinLoadBalancingPolicy") {

    it("should create QueryPlan in round-robin fashion") {
      val nodes = policy.newQueryPlan.nodes
      nodes.hasNext should be(true)
      nodes.next() should be(node(1111))
      nodes.hasNext should be(true)
      nodes.next() should be(node(2222))
      nodes.hasNext should be(false)
    }

    it("should pick up second after index is incremented") {
      policy.newQueryPlan //First plan
      val nodes = policy.newQueryPlan.nodes //Second one
      nodes.hasNext should be(true)
      nodes.next() should be(node(2222))
      nodes.hasNext should be(true)
      nodes.next() should be(node(1111))
      nodes.hasNext should be(false)
    }

    it("should not effect individual QueryPlans for intermediate index increments") {
      val nodes = policy.newQueryPlan.nodes
      nodes.hasNext should be(true)
      nodes.next() should be(node(1111))
      policy.index += 1
      nodes.hasNext should be(true)
      nodes.next() should be(node(2222))
      nodes.hasNext should be(false)
    }

    it("should check int overflow for index") {
      policy.index = Int.MaxValue - 10000
      policy.newQueryPlan
      policy.index should be(0)
    }

    it("should give QueryPlan with updated host list when new host added") {
      // Don't add this node if it's in livehosts list
      policy.onNodeAdded(node(3333))
      policy.onNodeAdded(node(3333))
      val nodes = policy.newQueryPlan.nodes

      nodes.hasNext should be(true)
      nodes.next() should be(node(1111))
      nodes.hasNext should be(true)
      nodes.next() should be(node(2222))
      nodes.hasNext should be(true)
      nodes.next() should be(node(3333))
      nodes.hasNext should be(false)
    }

    it("should give QueryPlan with updated host list when host is up") {
      // Don't add this node if it's in livehosts list
      policy.onNodeUp(node(3333))
      policy.onNodeUp(node(3333))
      val nodes = policy.newQueryPlan.nodes

      nodes.hasNext should be(true)
      nodes.next() should be(node(1111))
      nodes.hasNext should be(true)
      nodes.next() should be(node(2222))
      nodes.hasNext should be(true)
      nodes.next() should be(node(3333))
      nodes.hasNext should be(false)
    }

    it("should give QueryPlan with updated host list when host is removed") {
      policy.onNodeRemoved(node(1111))
      val nodes = policy.newQueryPlan.nodes
      nodes.hasNext should be(true)
      nodes.next() should be(node(2222))
      nodes.hasNext should be(false)
    }

    it("should give QueryPlan with updated host list when host is down") {
      policy.onNodeDown(node(1111))
      val nodes = policy.newQueryPlan.nodes
      nodes.hasNext should be(true)
      nodes.next() should be(node(2222))
      nodes.hasNext should be(false)
    }
  }
}
