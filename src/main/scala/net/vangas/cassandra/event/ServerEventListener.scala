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

package net.vangas.cassandra.event

import akka.actor.{ActorLogging, Actor}
import akka.util.ByteString
import net.vangas.cassandra.message.Event
import net.vangas.cassandra.{ResponseFrame, Factory}

class ServerEventListener(responseFrameFactory: Factory[ByteString, ResponseFrame] = ResponseFrame)
  extends Actor with ActorLogging {
  import context.system

  def receive = {
    case data: ByteString =>
      val ResponseFrame(_, body) = responseFrameFactory(data)
      body match {
        case event: Event => system.eventStream.publish(event)
        case x => log.warning("Unknown event body [{}]", x)
      }
  }

}
