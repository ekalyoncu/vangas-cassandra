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
import net.vangas.cassandra._
import net.vangas.cassandra.ColumnDefinition

class PreparedSpec extends FunSpec {

  describe("Prepared") {
    it("should create prepared from byteString") {
      val id = "PREPARED_ID"
      val metaData = new MetaDataBSBuilder()
        .withFlag(0)
        .withColumnCount(1)
        .withColumnDefinition("keyspace1", "table1", "column_name1", TYPE_INT)
        .build
      val resultMetaData = new MetaDataBSBuilder()
        .withFlag(0)
        .withColumnCount(1)
        .withColumnDefinition("keyspace1", "table1", "column_name1", TYPE_INT)
        .build

      val data = new ByteStringBuilder()
        .putInt(PREPARED)
        .putShort(id.getBytes.length)
        .putBytes(id.getBytes)
        .append(metaData)
        .append(resultMetaData)
        .result()
      val result = Result(data)
      result.body shouldBe a [Prepared]
      val prepared = result.body.asInstanceOf[Prepared]
      prepared.id.id should be(id.getBytes)
      val columnDefs = IndexedSeq(ColumnDefinition("keyspace1", "table1", "column_name1", INT))
      prepared.metaData should be (MetaData(false, false, false, 1, None, null, null, columnDefs))
      prepared.resultMetaData should be (MetaData(false, false, false, 1, None, null, null, columnDefs))
    }
  }

}
