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

import akka.actor.Actor._
import akka.actor._
import net.vangas.cassandra._
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.message.StatusChangeType._
import net.vangas.cassandra.message.TopologyChangeType._
import net.vangas.cassandra.message.{Query, StatusChangeEvent, TopologyChangeEvent}
import net.vangas.cassandra.util.NodeUtils.toNode

import scala.collection.mutable

class ConnectionPoolManager(sessionId: Int,
                            keyspace: String,
                            nodes: Seq[InetSocketAddress],
                            config: Configuration)
  extends Actor with Stash with ActorLogging { this: CPManagerComponents =>

  var isReady = false
  val pools = new mutable.HashMap[InetSocketAddress, RoundRobinConnectionPool] withDefault(_ => new RoundRobinConnectionPool)

  context.system.eventStream.subscribe(self, classOf[ConnectionReady])
  context.system.eventStream.subscribe(self, classOf[TopologyChangeEvent])
  context.system.eventStream.subscribe(self, classOf[StatusChangeEvent])

  nodes.foreach(createConnectionActors)

  override def preStart(): Unit = log.info(s"Starting ConnectionPoolManager-$sessionId actor...")


  override def postStop(): Unit = {
    log.info(s"Stopping ConnectionPoolManager-$sessionId and all connections in it...")
  }

  def receive = connectionLifeCycle orElse stashing

  ///////////////////ACTOR RECEIVE//////////////////////////
  def connectionLifeCycle: Receive = serverEvents orElse {
    case ConnectionReady(connection, node) => {
      def addToPool() {
        val pool = pools(node)
        pool.addConnection(connection)
        pools += node -> pool
      }
      log.info("Connection[{}] for node[{}] is ready.", connection, node)
      connection.tell(Query(s"USE $keyspace", config.queryConfig.toQueryParameters()), noSender)
      context.watch(connection)
      addToPool()
      becomeReady()
    }

    case Terminated(connection) =>
      log.info("Connection[{}] is dead!", connection)
      pools.foreach { case(node, pool) =>
        pool.removeConnection(connection)
      }
  }

  def ready: Receive = connectionLifeCycle orElse {
    case GetConnectionFor(node) =>
      pools(node).nextConnection match {
        case Some(connection) => sender ! ConnectionReceived(connection, node)
        case None => sender ! NoConnectionFor(node)
      }

    case PrepareOnAllNodes(prepare, exceptNode) =>
      log.debug("Preparing query[{}] on all nodes except [{}]", prepare, exceptNode)
      pools.foreach{ case(node, pool) => if (node != exceptNode) {
          pool.nextConnection.foreach(_.tell(prepare, noSender))
        }
      }
  }

  def serverEvents: Receive = {
    case TopologyChangeEvent(NEW_NODE, address) =>
      log.info("New node[{}] is being added pool...", address)
      addNewNode(toNode(address, config.port))

    case StatusChangeEvent(UP, address) =>
      log.info("Node[{}] is up and being added pool...", address)
      addNewNode(toNode(address, config.port))

    case TopologyChangeEvent(REMOVED_NODE, address) =>
      log.info("Node[{}] is removed from cluster and being removed from pool...", address)
      removeNodeAndConnections(toNode(address, config.port))

    case StatusChangeEvent(DOWN, address) =>
      log.info("Node[{}] is down and being removed from pool...", address)
      removeNodeAndConnections(toNode(address, config.port))
  }

  def stashing: Receive = {
    case msg =>
      log.debug("Stashing message:[{}]", msg)
      try { stash() } catch { case e: StashOverflowException => becomeReady() }
  }
  ///////////////////ACTOR RECEIVE END////////////////////////

  private def becomeReady(): Unit = {
    if (!isReady) {
      isReady = true
      unstashAll()
      context become ready
      log.debug("ConnectionPoolManager-{} is ready for requests (Unstashed all messages).", sessionId)
    }
  }

  private def createConnectionActors(node: InetSocketAddress): Unit = {
    for(i <- 1 to config.connectionsPerNode) {
      context.actorOf(Props(createConnection(config.queryTimeout, config.connectionTimeout, node)))
    }
  }

  private def addNewNode(node: InetSocketAddress): Unit = {
    if (pools.contains(node)){
      removeNodeAndConnections(node)
    }
    createConnectionActors(node)
  }

  private def removeNodeAndConnections(node: InetSocketAddress): Unit = {
    pools.get(node).foreach(_.killConnections())
    pools.remove(node)
  }
}

//Not Thread-safe should be used only in actor
private[connection] class RoundRobinConnectionPool {
  private[connection] var index = 0
  private[connection] val connections = new mutable.ListBuffer[ActorRef]
  
  def hasConnection: Boolean = connections.nonEmpty

  def nextConnection: Option[ActorRef] = {
    if (hasConnection) {
      val connection = connections(index % connections.size)
      index += 1
      if (index > Int.MaxValue - 10000) {
        //Overflow protection
        index = 0
      }
      Option(connection)
    } else {
      None
    }
  }

  def addConnection(connection: ActorRef): Unit = {
    if (!connections.contains(connection)) {
      connections += connection
    }
  }

  def removeConnection(connection: ActorRef): Unit = {
    connections -= connection
  }

  def killConnections(): Unit = {
    connections.foreach(_ ! PoisonPill)
  }

}