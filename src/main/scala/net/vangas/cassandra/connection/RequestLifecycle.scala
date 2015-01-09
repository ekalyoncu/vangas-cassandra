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

import akka.actor._
import net.vangas.cassandra.CassandraConstants._
import net.vangas.cassandra._
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.error.{RequestErrorCode, RequestError}
import net.vangas.cassandra.message._
import scala.collection.mutable

/**
 * Stateful actor which is responsible for the lifecycle of a request
 * till successful response or error back to requester.
 *
 * @param loadBalancer LoadBalancer actor which creates query plan for each request
 * @param connectionPools ConnectionPool actor which keeps connectionPools per host
 */
class RequestLifecycle(loadBalancer: ActorRef,
                       connectionPools: ActorRef,
                       config: Configuration,
                       debugMode: Boolean = false) extends Actor with ActorLogging {

  implicit val queryConfig = config.queryConfig

  val triedNodes = new mutable.ListBuffer[InetSocketAddress]()
  var currentConnection: Option[ActorRef] = None

  context.setReceiveTimeout(config.queryTimeout)

  context.system.eventStream.subscribe(self, classOf[ConnectionDefunct])

  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  if (debugMode) {
    context.system.eventStream.publish(RequestLifecycleStarted)
  }

  ////////////////////ACTOR RECEIVE////////////////////////
  def receive = {
    case statement: Statement =>
      log.debug("Started request lifecycle. Statement[{}].", statement)
      val requester = sender()
      context become readyForQueryPlan(RequestContext(requester, statement))
      loadBalancer ! CreateQueryPlan

    case ReceiveTimeout =>
      log.error("Query is timed out!")
      context stop self
  }

  def readyForQueryPlan(ctx: RequestContext): Receive = {
    case QueryPlan(nodes) =>
      context become new ReadyForResponse(ctx, nodes).receive
      tryNextNode(ctx.requester, nodes)

    case ReceiveTimeout =>
      log.error("Query is timed out!")
      context stop self
  }

  class ReadyForResponse(ctx: RequestContext, nodes: Iterator[InetSocketAddress]) {

    def receive: Receive = readyToTryNextNode orElse readyForResponse

    private def readyForResponse: Receive = {
      case ConnectionReceived(connection, node) =>
        triedNodes += node
        currentConnection = Option(connection)
        context.watch(connection)
        val request = ctx.statement.toRequestMessage
        log.debug("Sending request[{}] to connection[{}] on node[{}]...", request, connection.path, node)
        connection ! request

      case NodeAwareError(Error(UNPREPARED, msg), node) =>
        ctx.statement match {
          case bound: BoundStatement =>
            log.debug("Query[{}] is not prepared on node[{}]. Preparing query on it but not executing", bound.originalQuery, node)
            currentConnection.foreach(_ ! Prepare(bound.originalQuery))
            tryNextNode(ctx.requester, nodes)
          case _ =>
            val err = "Error is UNPREPARED but statement is not BoundStatement"
            log.error(err)
            ctx.requester ! RequestError(RequestErrorCode.UNPREPARED_WITH_INCONSISTENT_STATEMENT, err)
            context stop self
        }

      case requestError: NodeAwareError =>
        //debug level because it's already logged as error in RequestHandler
        log.debug("Request error[{}] on host[{}] happened. Escalating to the requester", requestError.error, requestError.node)
        ctx.requester ! RequestError(requestError.error)
        context stop self

      case prepared @ ExPrepared(_, query, node) =>
        ctx.requester ! prepared
        connectionPools ! PrepareOnAllNodes(Prepare(query), node)
        context stop self

      case ReceiveTimeout =>
        log.error("Query is timed out!")
        context stop self

      case result: Result =>
        ctx.requester ! ResultSet(result, ExecutionInfo(triedNodes.toSeq))
        log.debug("Sending result back to requester")
        context stop self

      case responseMessage: ResponseMessage =>
        ctx.requester ! responseMessage
        log.debug("Sending server response[{}] back to requester", responseMessage.getClass)
        context stop self

      case unknownMsg =>
        //This should not happen!
        log.error("Got unknown message[{}].", unknownMsg)
        context stop self
    }

    private def readyToTryNextNode: Receive = {
      case NoConnectionFor(node) =>
        log.debug("There is no live connection for host[{}]", node)
        tryNextNode(ctx.requester, nodes)

      case ConnectionDefunct(connection, node) =>
        if (currentConnection.contains(connection)) {
          log.debug("Defunct connection[{}] on node[{}]. Trying next host...", connection, node)
          tryNextNode(ctx.requester, nodes)
        }

      case Terminated(connection) =>
        log.debug("Connection[{}] is closed. Trying next host...", connection.path)
        tryNextNode(ctx.requester, nodes)

      case NodeAwareError(Error(SERVER_ERROR | OVERLOADED | BOOTSTRAPPING, msg), node) =>
        log.debug("Error on host[{}], Error message: {}. Trying next host...", node, msg)
        tryNextNode(ctx.requester, nodes)

      case MaxStreamIdReached(connection) =>
        log.debug("Connection[{}] has reached its max stream id. Trying next host...", connection)
        tryNextNode(ctx.requester, nodes)
    }
  }
  ////////////////////ACTOR RECEIVE ENDS////////////////////////

  private def tryNextNode(requester: ActorRef, nodes: Iterator[InetSocketAddress]): Unit = {
    currentConnection.foreach(context.unwatch) //Unwatch previous connection
    if (nodes.hasNext) {
      val node = nodes.next()
      log.debug("Trying next node[{}]", node)
      connectionPools ! GetConnectionFor(node)
    } else {
      log.error("All hosts in queryplan are tried and none of them was successful! Killing RequestLifecycle...")
      requester ! RequestError(RequestErrorCode.NO_HOST_AVAILABLE, "All hosts in queryplan are queried and none of them was successful")
      context stop self
    }
  }

}

case class RequestContext(requester: ActorRef, statement: Statement)