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
import akka.actor._
import akka.actor.Actor.noSender
import net.vangas.cassandra._
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.message.{Prepare, Query, RequestMessage}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._


class ConnectionRouter(keyspace: String,
                       nodes: Seq[InetSocketAddress],
                       config: Configuration,
                       connectionPool: ConnectionPool = new DefaultConnectionPool())
  extends Actor with Stash with ActorLogging { this: ConnectionRouterComponents =>

  var isReady = false
  val queryTimeout = FiniteDuration(config.queryTimeout, SECONDS)
  val connectionTimeout = FiniteDuration(config.connectionTimeout, SECONDS)

  context.system.eventStream.subscribe(self, classOf[ConnectionReady])
  context.system.eventStream.subscribe(self, classOf[MaxStreamIdReached])

  nodes.foreach{ node =>
    for(i <- 1 to config.connectionsPerNode) {
      context.actorOf(Props(createConnection(queryTimeout, connectionTimeout, node)), s"${node.getHostName}_connection$i")
    }
  }

  override def preStart(): Unit = log.info("Starting connection router actor...")


  override def postStop(): Unit = {
    log.info("Stopping router and all connections in it...")
  }

  def receive = connectionLifeCycle orElse stashing

  def connectionLifeCycle: Receive = {
    case ConnectionReady(connection, node) =>
      log.info("Connection[{}] to node[{}] is ready.", connection, node)
      connection.tell(Query(s"USE $keyspace", config.queryConfig.toQueryParameters()), noSender)
      context.watch(connection)
      connectionPool.addConnection(connection, node)
      if (!isReady) {
        isReady = true
        unstashAll()
        context become ready
        log.debug("ConnectionRouter is ready for requests (Unstashed all messages).")
      }

    case Terminated(connection) =>
      log.info("Connection[{}] is died!", connection)
      connectionPool.removeConnection(connection)
      if (connectionPool.hasNoConnection) {
        log.warning("There is no active connection in pool!")
        isReady = false
        context.unbecome()
      }

    case CloseRouter =>
      context stop self
  }

  def stashing: Receive = {
    case msg =>
      log.debug("Stashing message:[{}]", msg)
      stash()
  }

  def ready: Receive = connectionLifeCycle orElse {
    case request: RequestMessage =>
      val nextConnection = connectionPool.next
      log.debug("Sending request[{}] to connection[{}]...", request, nextConnection)
      nextConnection.forward(request)

    case max @ MaxStreamIdReached(request, requester, busyConnection, numOfRetries) if numOfRetries > 0 =>
      log.debug("Retrying request due to MaxStreamIdReached[{}]", max)
      val next = connectionPool.next
      val nextConn = if (next == busyConnection) connectionPool.next else next
      nextConn.tell(RetryFailedRequest(request, numOfRetries - 1), requester)

    case PrepareOnAllNodes(query, exceptThisNode) =>
      log.debug("Preparing query[{}] on all nodes except [{}]", query, exceptThisNode)
      connectionPool.executeOnAllNodes{ case (node, connection) =>
        if (node != exceptThisNode) connection.tell(Prepare(query), noSender)
      }
  }
}

trait ConnectionPool {
  type Entity = (InetSocketAddress, ActorRef)

  def hasConnection: Boolean
  def hasNoConnection: Boolean
  def addConnection(connection: ActorRef, node: InetSocketAddress)
  def removeConnection(connection: ActorRef)
  def next: ActorRef
  def executeOnAllNodes(func: Entity => Unit)
}


/**
 * NOT THREAD-SAFE. Should be used only in Actor instance.
 */
class DefaultConnectionPool extends ConnectionPool {
  private var counter = -1

  private[connection] val connectionsPerNode =
    mutable.Map.empty[InetSocketAddress, mutable.Buffer[ActorRef]].withDefault(_ => new ArrayBuffer[ActorRef]())

  private[connection] val connections = mutable.Buffer.empty[ActorRef]

  def hasConnection: Boolean = connections.nonEmpty

  def hasNoConnection: Boolean = !hasConnection

  def addConnection(connection: ActorRef, node: InetSocketAddress) = {
    if (!connections.contains(connection)) {
      connections += connection
      val connectionsToNode = connectionsPerNode(node)
      connectionsToNode += connection
      connectionsPerNode += node -> connectionsToNode
    }
  }

  def removeConnection(connection: ActorRef): Unit = {
    connections -= connection
    connectionsPerNode.foreach{ case(_, connectionBuffer) =>
      connectionBuffer -= connection
    }
  }

  //Round-Robin
  def next: ActorRef = if (hasConnection) connections(inc % connections.size) else null

  def executeOnAllNodes(func: Entity => Unit): Unit = {
    connectionsPerNode.foreach{ case (node, connectionBuffer) =>
      func((node, connectionBuffer.head))
    }
  }

  private def inc = {
    counter += 1
    counter
  }
}