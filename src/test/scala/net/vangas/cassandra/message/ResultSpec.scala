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
import akka.util.ByteStringBuilder
import net.vangas.cassandra.CassandraConstants._
import net.vangas.cassandra.byteOrder

class ResultSpec extends FunSpec {

  describe("Result") {
    it("should have Void kind") {
      val result = Result(new ByteStringBuilder().putInt(VOID).result())
      result should be(Result(Void))
    }

    it("should have SET_KEYSPACE kind") {
      val keyspaceName = "keyspace1"
      val data = new ByteStringBuilder()
        .putInt(SET_KEYSPACE)
        .putShort(keyspaceName.getBytes.length)
        .putBytes(keyspaceName.getBytes)
        .result()
      val result = Result(data)
      result should be(Result(SetKeyspace(keyspaceName)))
    }
  }

}
