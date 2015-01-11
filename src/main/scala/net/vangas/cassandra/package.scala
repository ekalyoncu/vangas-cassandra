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

package net.vangas

import java.nio.ByteOrder
import akka.actor.ActorRef
import java.net.InetSocketAddress
import akka.util.ByteString
import net.vangas.cassandra.message.{Prepare, RequestMessage}
import org.joda.time.DateTime

package object cassandra {

  type JUUID = java.util.UUID

  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  case class RequestStream(requester: ActorRef, originalRequest: RequestMessage, creationTime: DateTime = DateTime.now())
  case class ReceivedData(data: ByteString, requestStream: RequestStream)
  case class ConnectionReady(connection: ActorRef, nodeAddress: InetSocketAddress)
  case class ConnectionDefunct(connection: ActorRef, nodeAddress: InetSocketAddress)

  case object RequestLifecycleStarted

  case class MaxStreamIdReached(connection: ActorRef)
  case class PrepareOnAllNodes(prepare: Prepare, exceptThisNode: InetSocketAddress)

  case object CreateQueryPlan
  case class QueryPlan(nodes: Iterator[InetSocketAddress])
  case class GetConnectionFor(node: InetSocketAddress)
  case class ConnectionReceived(connection: ActorRef, node: InetSocketAddress)
  case class NoConnectionFor(node: InetSocketAddress)

}
