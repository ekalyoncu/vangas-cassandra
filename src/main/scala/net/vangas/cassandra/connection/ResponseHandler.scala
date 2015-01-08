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

import net.vangas.cassandra.message._
import akka.actor._
import java.net.InetSocketAddress
import net.vangas.cassandra._
import akka.util.ByteString
import akka.io.Tcp.Connected
import net.vangas.cassandra.message.ExPrepared
import net.vangas.cassandra.message.Authenticate

class ResponseHandler(responseFrameFactory: Factory[ByteString, ResponseFrame] = ResponseFrame)
  extends Actor with ActorLogging { this: ResponseHandlerComponents =>

  val authentication = context.actorOf(Props(createAuthentication))
  var cassandraConnection: ActorRef = _
  var node: InetSocketAddress = _

  def receive = {
    case Connected(remote, local) =>
      node = remote
      cassandraConnection = sender()

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

        case auth @ Authenticate(token) =>
          log.info("Authenticating...")
          authentication tell(auth, cassandraConnection)

        case x =>
          log.error(s"Unknown result body: [$body]")
      }
  }

}