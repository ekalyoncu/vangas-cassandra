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

package net.vangas.cassandra.message

import org.scalatest.FunSpec
import org.scalatest.Matchers._
import net.vangas.cassandra.{ConsistencyLevel, QueryParameters}
import net.vangas.cassandra.byteOrder

class QuerySpec extends FunSpec {

  describe("Query") {
    it("should serialize") {
      val q = "query_string"
      val query = Query(q, new QueryParameters(ConsistencyLevel.ONE, Seq("a"), false, 2))
      val serializedQuery = query.serialize.iterator

      serializedQuery.getInt should be(q.getBytes.length)
      val queryStringBytes = new Array[Byte](q.getBytes.length)
      serializedQuery.getBytes(queryStringBytes)

      serializedQuery.getShort should be(1)
      serializedQuery.getByte should be((0x01 | 0x04).toByte)
      serializedQuery.getShort should be(1)
      val stringBytes = "a".getBytes

      serializedQuery.getInt should be(stringBytes.length)
      val valuesBytes = new Array[Byte](stringBytes.length)
      serializedQuery.getBytes(valuesBytes)
      valuesBytes should be(stringBytes)
      serializedQuery.getInt should be(2)
    }
  }

}
