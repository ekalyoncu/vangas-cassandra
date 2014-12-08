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


import org.scalatest.{BeforeAndAfter, FunSpec}
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.when
import org.mockito.Matchers.any
import akka.util.{ByteIterator, ByteStringBuilder}
import net.vangas.cassandra._
import java.nio.ByteBuffer
import net.vangas.cassandra.CassandraConstants.TYPE_INT

class RowsSpec extends FunSpec with BeforeAndAfter {


  /**
   *
   * <metadata><rows_count><rows_content>
   *
   * <metadata> is composed of:
   *  <flags><columns_count>[<paging_state>][<global_table_spec>?<col_spec_1>...<col_spec_n>]
   *
   */
  describe("Rows") {
    it("should create rows from byteString") {
      val rowsCount = 2
      val row1Value = ByteBuffer.allocate(4).putInt(11).array()
      val row2Value = ByteBuffer.allocate(4).putInt(22).array()
      val metaData = new MetaDataBSBuilder()
        .withFlag(0)
        .withColumnCount(1)
        .withColumnDefinition("keyspace1", "table1", "column_name1", TYPE_INT)
        .build
      val rowsContent = new ByteStringBuilder()
        .putInt(row1Value.length).putBytes(row1Value)
        .putInt(row2Value.length).putBytes(row2Value)
        .result()

      val rowsResultBody =
        new ByteStringBuilder().append(metaData).putInt(rowsCount).append(rowsContent).result().iterator

      val rows = Rows(rowsResultBody)

      rows.content.size should be(2)
      rows.metaData.globalTableSpec should be(false)
      rows.metaData.hasMorePages should be(false)
      rows.metaData.noMetaData should be(false)
      rows.metaData.pagingState should be(None)
      rows.metaData.columnsCount should be(1)
      rows.metaData.keyspaceName should be(null)
      rows.metaData.tableName should be(null)
      rows.metaData.columns.size should be(1)
      rows.metaData.columns.head.keyspaceName should be("keyspace1")
      rows.metaData.columns.head.tableName should be("table1")
      rows.metaData.columns.head.name should be("column_name1")
      rows.metaData.columns.head.dataType.getClass should be(INT.getClass)

      rows.count should be(2)
      rows.content.size should be(2)
    }

    it("should create rows with null column in row") {
      val rowsCount = 1
      val metaDataFactory = mock[Factory[ByteIterator, MetaData]]
      val metaData = MetaData(false, false, false, 1, None, null, null, null)
      when(metaDataFactory.apply(any())).thenReturn(metaData)
      val rowsContent = new ByteStringBuilder().putInt(-1).result()
      val body = new ByteStringBuilder().putInt(rowsCount).append(rowsContent).result().iterator
      val rows = Rows.apply(body, metaDataFactory)
      rows.content.size should be(1)
      rows.content.head.isNull(0) should be(true)
    }

    //TODO: Write following tests
    it("should create rows with pagingstate") { }
    it("should create rows with global_table_spec") { }
    it("should create rows with no_metadata flag") { }
  }

}
