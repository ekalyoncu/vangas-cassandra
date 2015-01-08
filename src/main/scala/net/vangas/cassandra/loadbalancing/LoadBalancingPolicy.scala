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

import java.net.InetSocketAddress

import net.vangas.cassandra.{ServerEvents, QueryPlan}

/**
 * This is responsible about providing list of nodes to be tried
 * until query is successfully executed on one of them.
 *
 * This class is used in LoadBalancer actor. Don't expose internal state of this class via QueryPlan object.
 */
trait LoadBalancingPolicy extends ServerEvents {

  def init(nodes: Seq[InetSocketAddress])

  /**
   * Creates new query plan for each query which contains list of nodes.
   * These nodes are tried until query is successfully executed.
   *
   * It's used by actor only which means this method will be called by one thread at a time.
   * @return new QueryPlan
   */
  def newQueryPlan: QueryPlan
}