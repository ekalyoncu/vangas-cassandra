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

package net.vangas.cassandra.util

import net.vangas.cassandra.message._
import net.vangas.cassandra.CassandraConstants._
import akka.util.ByteString
import net.vangas.cassandra.{Header, RequestFrame}

object FrameUtils {

  def startupFrame(streamId: Short): ByteString = serializeFrame(streamId, Startup)

  def serializeFrame(streamId: Short, body: RequestMessage): ByteString = {
    val opCode = body match {
      case Startup => STARTUP
      case _: Query => QUERY
      case _: Prepare => PREPARE
      case _: Execute => EXECUTE
      case x => throw new IllegalArgumentException(s"OpCode is not known for request[$x]")
    }
    RequestFrame(Header(VERSION_FOR_V3, FLAGS, streamId, opCode), body).serialize
  }

}
