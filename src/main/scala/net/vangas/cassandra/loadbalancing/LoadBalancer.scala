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

import akka.actor.Actor
import net.vangas.cassandra._
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.message.StatusChangeType._
import net.vangas.cassandra.message.TopologyChangeType._
import net.vangas.cassandra.message.{StatusChangeEvent, TopologyChangeEvent}
import net.vangas.cassandra.util.NodeUtils.toNode

class LoadBalancer(policy: LoadBalancingPolicy, config: Configuration) extends Actor {

  context.system.eventStream.subscribe(self, classOf[TopologyChangeEvent])
  context.system.eventStream.subscribe(self, classOf[StatusChangeEvent])

  def receive = loadBalance orElse serverEvents

  private def loadBalance: Receive = {
    case CreateQueryPlan =>
      sender ! policy.newQueryPlan
  }

  private def serverEvents: Receive = {
    case TopologyChangeEvent(NEW_NODE, address) =>
      policy.onNodeAdded(toNode(address, config.port))

    case TopologyChangeEvent(REMOVED_NODE, address) =>
      policy.onNodeRemoved(toNode(address, config.port))

    case StatusChangeEvent(UP, address) =>
      policy.onNodeUp(toNode(address, config.port))

    case StatusChangeEvent(DOWN, address) =>
      policy.onNodeDown(toNode(address, config.port))
  }
}