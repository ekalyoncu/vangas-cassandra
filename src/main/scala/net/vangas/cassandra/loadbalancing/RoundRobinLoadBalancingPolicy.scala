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

import net.vangas.cassandra.QueryPlan
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

class RoundRobinLoadBalancingPolicy extends LoadBalancingPolicy {

  val LOG = LoggerFactory.getLogger(classOf[RoundRobinLoadBalancingPolicy])

  private[loadbalancing] var initialized = false
  private[loadbalancing] var index = 0
  private[loadbalancing] var liveNodes = new ListBuffer[InetSocketAddress]

  def init(nodes: Seq[InetSocketAddress]): Unit = {
    if (initialized) {
      throw new IllegalStateException("RoundRobinLoadBalancingPolicy is already initialized!")
    }
    liveNodes ++= nodes
  }

  /**
   * Creates new query plan for each query which contains list of nodes.
   * These nodes are tried until query is successfully executed.
   *
   * It's only used by actor which means this method will be called by one thread at a time.
   * @return new QueryPlan
   */
  def newQueryPlan: QueryPlan = {
    val queryPlan = QueryPlan(new Iterator[InetSocketAddress] {
      var idx = index
      var remaining = liveNodes.size
      val nodesToRequest = Seq(liveNodes:_*) //Make immutable copy of liveNodes
      LOG.debug(s"Nodes:[${nodesToRequest.mkString(",")}] to be queried for new QueryPlan")

      override def hasNext: Boolean = remaining > 0
      override def next(): InetSocketAddress = {
        val node = nodesToRequest(idx % nodesToRequest.size)
        remaining -= 1
        idx += 1
        node
      }
    })
    incrementIndex()
    queryPlan
  }

  private def incrementIndex(): Unit = {
    index += 1
    if (index > Int.MaxValue - 10000) {
      //Overflow protection
      index = 0
      LOG.info("Index is overflowed. Set to zero.")
    }
  }


  def onNodeAdded(node: InetSocketAddress): Unit = {
    if (!liveNodes.contains(node)) {
      liveNodes += node
    }
  }

  def onNodeDown(node: InetSocketAddress): Unit = {
    liveNodes -= node
  }

  def onNodeUp(node: InetSocketAddress): Unit = onNodeAdded(node)

  def onNodeRemoved(node: InetSocketAddress): Unit = onNodeDown(node)

}
