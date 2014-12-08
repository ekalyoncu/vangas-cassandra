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

import org.scalatest.FunSpec
import org.scalatest.Matchers._
import ConsistencyLevel._
import java.nio.ByteBuffer

class QueryParametersSpec extends FunSpec {

  describe("QueryParameters") {
    it("should serialize") {
      val queryParams = new QueryParameters(ONE, Seq("abc", 1234), true, 10)
      val byteStringIter = queryParams.serialize.iterator
      byteStringIter.getShort should be(1)
      byteStringIter.getByte should be((0x01 | 0x02 | 0x04).toByte)
      byteStringIter.getShort should be(2)
      val stringBytes = "abc".getBytes
      val intBytes = ByteBuffer.allocate(4).putInt(1234).array()

      byteStringIter.getInt should be(stringBytes.length)
      val firstValueBytes = new Array[Byte](stringBytes.length)
      byteStringIter.getBytes(firstValueBytes)
      firstValueBytes should be(stringBytes)
      byteStringIter.getInt should be(4)
      val secondValueBytes = new Array[Byte](intBytes.length)
      byteStringIter.getBytes(secondValueBytes)
      secondValueBytes should be(intBytes)
      byteStringIter.getInt should be(10)
    }
  }

}
