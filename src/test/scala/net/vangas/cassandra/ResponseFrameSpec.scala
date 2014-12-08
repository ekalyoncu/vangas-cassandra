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

import net.vangas.cassandra.message._
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import akka.util.{ByteString, ByteStringBuilder}
import net.vangas.cassandra.CassandraConstants._

class ResponseFrameSpec extends FunSpec {

  private val FRAME_LENGTH = 1111

  describe("ResponseFrame") {
    it("should create error frame from bytestring") {
      val data = new ByteStringBuilder()
        .putByte(VERSION_FOR_V3)
        .putByte(FLAGS)
        .putShort(123)
        .putByte(ERROR)
        .putInt(FRAME_LENGTH)
        .putInt(PROTOCOL_ERROR)
        .putShort("Bla".length)
        .append(ByteString.fromString("Bla"))
        .result()

      val responseFrame = ResponseFrame(data)
      responseFrame should be (ResponseFrame(Header(VERSION_FOR_V3, FLAGS, 123, ERROR), Error(PROTOCOL_ERROR, "Bla")))
    }

    it("should create void result from bytestring") {

      val data = new ByteStringBuilder()
        .putByte(VERSION_FOR_V3)
        .putByte(FLAGS)
        .putShort(123)
        .putByte(RESULT)
        .putInt(FRAME_LENGTH)
        .putInt(VOID)
        .result()

      val responseFrame = ResponseFrame(data)
      responseFrame should be (ResponseFrame(Header(VERSION_FOR_V3, FLAGS, 123, RESULT), Result(Void)))
    }
  }

}
