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
import akka.event.Logging
import akka.io.Tcp._
import net.vangas.cassandra.exception.ConnectionException
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
 * @param node Cassandra node to be connected
 * @param streamContext Responsible for getting next available stream id, registering requester with stream id
 * @param streamIdExtractor Extracts streamId from response frame
 */
private[cassandra] class Connection(connectionTimeout: FiniteDuration,
                                    node: InetSocketAddress,
                                    streamContext: StreamContext,
                                    streamIdExtractor: StreamIdExtractor)
  extends Actor { this: ConnectionComponents =>
  import context.system

  val log = Logging(system.eventStream, "Connection_" + self.path.name)

  var cassandraConnection: ActorRef = _

  val responseHandler = context.actorOf(Props(createResponseHandlerActor(node)))

  ioManager ! Connect(node, timeout = Some(connectionTimeout))

  override def preStart(): Unit = log.info("Starting connection actor...")

  override def postStop(): Unit = {
    log.info("Stopping connection actor...")
    if (cassandraConnection != null) {
      cassandraConnection ! Close
    }
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {SupervisorStrategy.defaultDecider}

  def receive = {
    case c @ Connected(remote, local) =>
      log.info("Connected to node[{}] but connection still not ready to be used.", remote)
      cassandraConnection = sender()
      cassandraConnection ! Register(self)
      streamContext.registerStream(RequestStream(self, Startup)) match {
        case Some(streamId) =>
          cassandraConnection ! Write(startupFrame(streamId))
          context become connected
        case None =>
          context stop self
      }

    case CommandFailed(_: Connect) =>
      log.error("Cannot connect to node[{}]", node)
      context stop self
  }

  private[connection] def connected: Receive = {
    case Received(data) =>
      val streamId = streamIdExtractor.streamId(data)
      if (streamId == -1) { //Server-side events
        responseHandler ! data
      } else {
        streamContext.getRequester(streamId) match {
          case Some(requestStream) =>
            responseHandler ! ReceivedData(data, requestStream)
            streamContext.releaseStream(streamId)
          case _ => log.error("Server returned streamId[{}] which client didn't sent! Data: {}", streamId, data)
        }
      }

    case Ready =>
      system.eventStream.publish(ConnectionReady(self, node))
      log.info("Connection is ready to be used.")

    case query: Query => doRequest(query, sender())

    case prepare: Prepare => doRequest(prepare, sender())

    case execute: Execute => doRequest(execute, sender())

    case registerForEvents: RegisterForEvents =>
      log.info("Registering for event types: {}", registerForEvents.eventTypes)
      val requester = sender()
      streamContext.registerStream(RequestStream(null, null)) match {
        case Some(streamId) =>
          cassandraConnection ! Write(serializeFrame(streamId, registerForEvents))
          streamContext.releaseStream(streamId) //We don't need anymore
        case _ =>
          log.warning("Connection has reached max stream id limit for request:[{}]", registerForEvents)
          requester ! MaxStreamIdReached(self)
          streamContext.cleanExpiredStreams()
      }

    case CommandFailed(w: Write) =>
      val err = s"Command Failed. ACK:[${w.ack}]"
      log.error(err)
      system.eventStream.publish(ConnectionDefunct(self, node))
      throw new ConnectionException(err) //this will restart connection

    case _: ConnectionClosed => context stop self
  }

  private def doRequest(request: RequestMessage, requester: ActorRef) {
    streamContext.registerStream(RequestStream(requester, request)) match {
      case Some(streamId) => cassandraConnection ! Write(serializeFrame(streamId, request))
      case _ =>
        log.warning("Connection has reached max stream id limit for request:[{}]", request)
        requester ! MaxStreamIdReached(self)
        streamContext.cleanExpiredStreams()
    }
  }
}