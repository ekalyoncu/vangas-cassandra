
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

package net.vangas.cassandra

import akka.util.{ByteStringBuilder, ByteString}
import net.vangas.cassandra.message._

case class Header(version: Byte, flags: Byte, streamId: Short, opCode: Byte) {
  def serialize: ByteString =
    new ByteStringBuilder()
      .putByte(version)
      .putByte(flags)
      .putShort(streamId)
      .putByte(opCode).result()
}

/**
 * The CQL binary protocol is a frame based protocol. Frames (protocol v3) are defined as:
 *
 *       0         8        16        24        32         40
 *       +---------+---------+---------+---------+---------+
 *       | version |  flags  |      stream       | opcode  |
 *       +---------+---------+---------+---------+---------+
 *       |                length                 |
 *       +---------+---------+---------+---------+
 *       |                                       |
 *       .            ...  body ...              .
 *       .                                       .
 *       .                                       .
 *       +----------------------------------------
 *
 */
case class RequestFrame(header: Header, body: RequestMessage) {
  def serialize: ByteString = {
    val bodyByteString = body.serialize
    new ByteStringBuilder()
      .append(header.serialize)
      .putInt(bodyByteString.length)
      .append(bodyByteString)
      .result()
  }
}

case class ResponseFrame(header: Header, body: ResponseMessage)

object ResponseFrame extends Factory[ByteString, ResponseFrame] with StreamIdExtractor {
  import CassandraConstants._
  
  def apply(data: ByteString): ResponseFrame = {
    val byteIter = data.iterator
    val header = Header(byteIter.getByte, byteIter.getByte, byteIter.getShort, byteIter.getByte)
    val length = byteIter.getInt
    val body = byteIter.toByteString

    header.opCode match {
      case READY => ResponseFrame(header, Ready)
      case RESULT => ResponseFrame(header, Result(body))
      case ERROR => ResponseFrame(header, Error(body))
      case AUTHENTICATE => ResponseFrame(header, Authenticate(body))
      case x => throw new IllegalArgumentException(s"Unknown opcode[$x]")
    }
  }

  def streamId(data: ByteString): Short = data.iterator.drop(2).getShort
}

trait StreamIdExtractor {
  def streamId(data: ByteString): Short
}