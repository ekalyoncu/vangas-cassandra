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

import net.vangas.cassandra.RequestStream
import org.joda.time.{Seconds, DateTime}
import scala.collection.mutable
import scala.concurrent.duration._

trait StreamContext {
  def lastUsedId: Short
  def getRequester(id: Short): Option[RequestStream]
  def registerStream(requestStream: RequestStream): Option[Short]
  def releaseStream(id: Short): Unit
  def cleanExpiredStreams(): Unit
}

/**
 * NOT THREAD-SAFE. Should be used only in Actor instance.
 */
class DefaultStreamContext(ttlForStream: FiniteDuration) extends StreamContext {
  private val MAX_SIMULTANEOUS_STREAM = 128

  private[connection] val streamIds = new Array[Boolean](MAX_SIMULTANEOUS_STREAM)
  private[connection] var streams = mutable.Map.empty[Short, RequestStream]
  private[connection] var _lastUsedId: Short = -1

  def lastUsedId: Short = _lastUsedId

  def getRequester(id: Short): Option[RequestStream] = streams.get(id)

  def registerStream(requestStream: RequestStream): Option[Short] = {
    newId.map { id =>
      streams += id -> requestStream
      id
    }
  }

  def releaseStream(id: Short): Unit = {
    streams -= id
    streamIds(id) = false
  }

  def cleanExpiredStreams(): Unit = {
    val now = DateTime.now()
    val ttl = ttlForStream.plus(1 second).toSeconds
    def expired(streamTime: DateTime): Boolean = Seconds.secondsBetween(streamTime, now).getSeconds >= ttl

    val expiredIds = streams.collect{ case(id, req) if expired(req.creationTime) => id }
    expiredIds.foreach { id =>
      streams -= id
      streamIds(id) = false
    }
  }

  private def newId: Option[Short] = {
    val nextAvailableStreamId = streamIds.indexOf(false)
    if (nextAvailableStreamId < 0) {
      None
    } else {
      streamIds(nextAvailableStreamId) = true
      _lastUsedId = nextAvailableStreamId.toShort
      Option(nextAvailableStreamId.toShort)
    }
  }


}
