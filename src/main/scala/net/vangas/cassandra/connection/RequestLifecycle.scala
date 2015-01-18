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
 * @param connectionPoolManager ConnectionPoolManager actor which keeps connectionPools per host
 */
class RequestLifecycle(loadBalancer: ActorRef,
                       connectionPoolManager: ActorRef,
                       config: Configuration) extends Actor with ActorLogging {

  implicit val queryConfig = config.queryConfig

  val triedNodes = new mutable.ListBuffer[InetSocketAddress]()
  var currentConnection: Option[ActorRef] = None

  context.setReceiveTimeout(config.queryTimeout)

  context.system.eventStream.subscribe(self, classOf[ConnectionDefunct])

  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  ////////////////////ACTOR RECEIVE////////////////////////
  def receive = {
    case ctx: RequestContext =>
      log.debug("Started request lifecycle. Statement[{}].", ctx.statement)
      context become readyForQueryPlan(ctx)
      loadBalancer ! CreateQueryPlan

    case ReceiveTimeout =>
      log.error("Query is timed out!")
      context stop self
  }

  def readyForQueryPlan(ctx: RequestContext): Receive = {
    case QueryPlan(nodes) =>
      context become new ReadyForResponse(ctx, nodes).receive
      tryNextNode(ctx, nodes)

    case ReceiveTimeout =>
      log.error("Query is timed out!")
      context stop self
  }

  class ReadyForResponse(ctx: RequestContext, nodes: Iterator[InetSocketAddress]) {

    def receive: Receive = handleResponses orElse handleErrors orElse unknown

    private def handleResponses: Receive = {
      case ConnectionReceived(connection, node) =>
        triedNodes += node
        currentConnection = Option(connection)
        context.watch(connection)
        val request = ctx.statement.toRequestMessage
        log.debug("Sending request[{}] to connection[{}] on node[{}]...", request, connection.path, node)
        connection ! request

      case prepared @ ExPrepared(_, query, node) =>
        ctx.respond(ResponseContext(prepared))
        connectionPoolManager ! PrepareOnAllNodes(Prepare(query), node)
        context stop self

      case result: Result =>
        ctx.respond(ResponseContext(ResultSet(result, ExecutionInfo(triedNodes.toSeq))))
        log.debug("Sending result back to requester")
        context stop self

      case responseMessage: ResponseMessage =>
        ctx.respond(ResponseContext(responseMessage))
        log.debug("Sending server response[{}] back to requester", responseMessage.getClass)
        context stop self
    }

    private def handleErrors: Receive = {
      case NoConnectionFor(node) =>
        log.debug("There is no live connection for host[{}]", node)
        tryNextNode(ctx, nodes)

      case ConnectionDefunct(connection, node) =>
        if (currentConnection.contains(connection)) {
          log.debug("Defunct connection[{}] on node[{}]. Trying next host...", connection, node)
          tryNextNode(ctx, nodes)
        }

      case Terminated(connection) =>
        log.debug("Connection[{}] is closed. Trying next host...", connection.path)
        tryNextNode(ctx, nodes)

      case NodeAwareError(Error(UNPREPARED, msg), node) =>
        ctx.statement match {
          case bound: BoundStatement =>
            log.debug("Query[{}] is not prepared on node[{}]. Preparing query on it but not executing", bound.originalQuery, node)
            currentConnection.foreach(_ ! Prepare(bound.originalQuery))
            tryNextNode(ctx, nodes)
          case _ =>
            val err = "Error is UNPREPARED but statement is not BoundStatement"
            log.error(err)
            ctx.respond(ResponseContext(RequestError(RequestErrorCode.UNPREPARED_WITH_INCONSISTENT_STATEMENT, err)))
            context stop self
        }

      case NodeAwareError(Error(SERVER_ERROR | OVERLOADED | BOOTSTRAPPING, msg), node) =>
        log.debug("Error on host[{}], Error message: {}. Trying next host...", node, msg)
        tryNextNode(ctx, nodes)

      case requestError: NodeAwareError =>
        //debug level because it's already logged as error in RequestHandler
        log.debug("Request error[{}] on host[{}] happened. Escalating to the requester", requestError.error, requestError.node)
        ctx.respond(ResponseContext(RequestError(requestError.error)))
        context stop self

      case MaxStreamIdReached(connection) =>
        log.debug("Connection[{}] has reached its max stream id. Trying next host...", connection)
        tryNextNode(ctx, nodes)

      case ReceiveTimeout =>
        log.error("Query is timed out!")
        context stop self
    }

    private def unknown: Receive = {
      case unknownMsg =>
        //This should not happen!
        log.error("Got unknown message[{}].", unknownMsg)
        context stop self
    }
  }
  ////////////////////ACTOR RECEIVE ENDS////////////////////////

  private def tryNextNode(requestCtx: RequestContext, nodes: Iterator[InetSocketAddress]): Unit = {
    currentConnection.foreach(context.unwatch) //Unwatch previous connection
    if (nodes.hasNext) {
      val node = nodes.next()
      log.debug("Trying next node[{}]", node)
      connectionPoolManager ! GetConnectionFor(node)
    } else {
      val err = "All hosts in queryplan are queried and none of them was successful."
      log.error(err)
      requestCtx.respond(ResponseContext(RequestError(RequestErrorCode.NO_HOST_AVAILABLE, err)))
      context stop self
    }
  }

}

//Continuation style request
case class RequestContext(statement: Statement, respond: ResponseContext => Unit)

case class ResponseContext(response: Either[RequestError, Any])

object ResponseContext {

  def apply(response: ResponseMessage): ResponseContext = ResponseContext(Right(response))
  
  def apply(resultSet: ResultSet): ResponseContext = ResponseContext(Right(resultSet))

  def apply(requestError: RequestError): ResponseContext = ResponseContext(Left(requestError))
}

