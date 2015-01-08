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

class LoadBalancer(policy: LoadBalancingPolicy) extends Actor {

  def receive = loadBalance orElse serverEvents

  private def loadBalance: Receive = {
    case CreateQueryPlan =>
      sender ! policy.newQueryPlan
  }

  private def serverEvents: Receive = {
    case NodeAdded(node) =>
      policy.onNodeAdded(node)

    case NodeRemoved(node) =>
      policy.onNodeRemoved(node)

    case NodeUp(node) =>
      policy.onNodeUp(node)

    case NodeDown(node) =>
      policy.onNodeDown(node)
  }
}