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

import akka.util.{ByteString, ByteStringBuilder}

/**
 * <flags><columns_count>[<paging_state>][<global_table_spec>?<col_spec_1>... <col_spec_n>]
 */
class MetaDataBSBuilder {

  val builder = new ByteStringBuilder()

  def withFlag(flag: Int): MetaDataBSBuilder = {
    builder.putInt(flag)
    this
  }

  def withColumnCount(count: Int): MetaDataBSBuilder = {
    builder.putInt(count)
    this
  }

  def withPagingState(bytes: ByteString): MetaDataBSBuilder = {
    builder.append(bytes)
    this
  }

  def withGlobalTableSpec(keyspace: String, tableName: String) : MetaDataBSBuilder = {
    builder.putShort(keyspace.getBytes.length).putBytes(keyspace.getBytes)
    builder.putShort(tableName.getBytes.length).putBytes(tableName.getBytes)
    this
  }

  def withColumnDefinition(keyspace: String, tableName: String, columnName: String, dataType: Int) : MetaDataBSBuilder = {
    builder.putShort(keyspace.getBytes.length).putBytes(keyspace.getBytes)
    builder.putShort(tableName.getBytes.length).putBytes(tableName.getBytes)
    builder.putShort(columnName.getBytes.length).putBytes(columnName.getBytes)
    builder.putShort(dataType)
    this
  }

  def build: ByteString = builder.result()

}