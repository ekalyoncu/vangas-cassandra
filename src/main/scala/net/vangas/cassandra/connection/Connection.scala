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

import akka.actor._
import akka.io.Tcp._
import scala.concurrent.duration._
import java.net.InetSocketAddress
import net.vangas.cassandra.util.FrameUtils
import FrameUtils._
import net.vangas.cassandra._
import net.vangas.cassandra.message._
import akka.io.Tcp.Connected
import net.vangas.cassandra.RequestStream
import net.vangas.cassandra.message.Execute
import akka.io.Tcp.Register
import akka.io.Tcp.Connect
import net.vangas.cassandra.ConnectionReady
import akka.io.Tcp.CommandFailed
import akka.io.Tcp.Received
import net.vangas.cassandra.message.Prepare
import net.vangas.cassandra.ReceivedData
import net.vangas.cassandra.message.Query

/**
 * This class is a connection to any cassandra node.
 * It is responsible to connect, send request frames to the node
 * and redirect received messages to the [[net.vangas.cassandra.connection.ResponseHandler]].
 *
 * @param nodeAddress Cassandra node to be connected
 * @param streamContext Responsible for getting next available stream id, registering requester with stream id
 * @param streamIdExtractor Extracts streamId from response frame
 */
private[cassandra] class Connection(connectionTimeout: FiniteDuration,
                                    nodeAddress: InetSocketAddress,
                                    streamContext: StreamContext,
                                    streamIdExtractor: StreamIdExtractor)
  extends Actor with ActorLogging { this: ConnectionComponents =>
  import context.system

  val MAX_RETRY_COUNT = 3

  var cassandraConnection: ActorRef = _

  val responseHandler = context.actorOf(Props(createResponseHandlerActor))

  var isReady = false

  ioManager ! Connect(nodeAddress, timeout = Some(connectionTimeout))

  override def preStart(): Unit = log.info("Starting connection actor...")

  override def postStop(): Unit = {
    log.info("Stopping connection actor...")
    if (cassandraConnection != null) {
      cassandraConnection ! Close
    }
  }

  def receive = {
    case c @ Connected(remote, local) =>
      log.info("Connected to remote address[{}]", remote)
      cassandraConnection = sender()
      responseHandler tell (c, cassandraConnection)
      cassandraConnection ! Register(self)
      streamContext.registerStream(RequestStream(self, Startup)) match {
        case Some(streamId) =>
          cassandraConnection ! Write(startupFrame(streamId))
          context become connected
        case None =>
          context stop self
      }


    case CommandFailed(_: Connect) =>
      log.error("Cannot connect to node[{}]", nodeAddress)
      context stop self
  }

  private[connection] def connected: Receive = {
    case Received(data) =>
      val streamId = streamIdExtractor.streamId(data)
      streamContext.getRequester(streamId) match {
        case Some(requestStream) =>
          responseHandler ! ReceivedData(data, requestStream)
          streamContext.releaseStream(streamId)
        case _ => log.error("Server returned streamId[{}] which client didn't sent! Data: {}", streamId, data)
      }

    case Ready =>
      isReady = true
      system.eventStream.publish(ConnectionReady(self, nodeAddress))

    case query: Query => doRequest(query, sender())

    case prepare: Prepare => doRequest(prepare, sender())

    case execute: Execute => doRequest(execute, sender())

    case RetryFailedRequest(originalRequest, numOfRetries) => doRequest(originalRequest, sender(), numOfRetries)

    case CommandFailed(w: Write) => log.error(s"Command Failed. ACK:[${w.ack}]")

    case CloseConnection => context stop self

    case _: ConnectionClosed => context stop self

    case IsConnectionReady => sender ! isReady
  }

  private def doRequest(request: RequestMessage, requester: ActorRef, numOfRetries: Int = MAX_RETRY_COUNT) {
    streamContext.registerStream(RequestStream(requester, request)) match {
      case Some(streamId) => cassandraConnection ! Write(serializeFrame(streamId, request))
      case _ =>
        log.warning("Connection has reached max stream id limit for request:[{}]", request)
        system.eventStream.publish(MaxStreamIdReached(request, requester, self, numOfRetries))
        streamContext.cleanExpiredStreams()
    }
  }
}