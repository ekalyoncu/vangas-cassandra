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
import akka.util.ByteString
import java.net.InetSocketAddress
import net.vangas.cassandra.message._
import net.vangas.cassandra._
import net.vangas.cassandra.message.ExPrepared
import net.vangas.cassandra.message.StatusChangeType._
import net.vangas.cassandra.message.TopologyChangeType._
import scala.concurrent.duration._

class ResponseHandler(node: InetSocketAddress,
                      responseFrameFactory: Factory[ByteString, ResponseFrame] = ResponseFrame) extends Actor {

  val log = Logging(context.system.eventStream, "ResponseHandler")

  def receive = {
    case ReceivedData(data, RequestStream(requester, originalRequest, _)) =>
      val ResponseFrame(header, body) = responseFrameFactory(data)
      body match {
        case r @ Ready =>
          log.info("Got Ready message from server[{}]", node)
          requester ! r

        case Result(prepared: Prepared) =>
          log.debug("Got Prepared[{}] from server[{}]. Original Request: [{}]", prepared, node, originalRequest)
          originalRequest match {
            case Prepare(query) => requester ! ExPrepared(prepared, query, node)
            case x => log.error("Got different original request[{}] than Prepare", x)
          }

        case r: Result =>
          log.debug("Got Result[{}] from server[{}]", r, node)
          requester ! r

        case error @ Error(code, msg) =>
          log.error("Error occurred. Code:[{}], Message: [{}], Original request: [{}], Node[{}]", code, msg, originalRequest, node)
          requester ! NodeAwareError(error, node)

        case x =>
          log.error(s"Unknown result body: [$body]")
      }

    case data: ByteString =>
      import context.dispatcher

      val ResponseFrame(_, body) = responseFrameFactory(data)
      body match {
        case nodeUp @ (TopologyChangeEvent(NEW_NODE, _) | StatusChangeEvent(UP, _)) =>
          log.info("Got node up event[{}]", nodeUp)
          //Cassandra sends event before node is up, so spec says to wait 1 second before trying to connect to node
          context.system.scheduler.scheduleOnce(1 second) {
            context.system.eventStream.publish(nodeUp)
          }

        case event: Event =>
          log.info("Got server event[{}]", event)
          context.system.eventStream.publish(event)

        case x => log.warning("Unknown event body [{}]", x)
      }
  }

}