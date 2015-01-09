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
import net.vangas.cassandra.message.SchemaChange
import net.vangas.cassandra.message.SetKeyspace

trait ResultSet extends Iterable[Row] {
  def executionInfo(): ExecutionInfo
}

/**
 * For Result kind: VOID, SET_KEYSPACE, SCHEMA_CHANGE
 */
class EmptyResultSet(val executionInfo: ExecutionInfo = ExecutionInfo()) extends ResultSet {
  def iterator: Iterator[Row] = Iterator.empty
}

class SinglePageResultSet(rows: Iterable[Row], val executionInfo: ExecutionInfo) extends ResultSet {
  def iterator: Iterator[Row] = rows.iterator
}

//TODO: next and hasNext should get data from server asynchronously and this is not the place to make async requests.
class PaginatedResultSet(rows: Rows) extends ResultSet {
  def iterator: Iterator[Row] = new Iterator[Row] {
    def hasNext: Boolean = ???

    def next(): Row = ???
  }
  def executionInfo(): ExecutionInfo = ???
}


object ResultSet {

  val empty = new EmptyResultSet

  def apply(result: Result, executionInfo: ExecutionInfo = ExecutionInfo()): ResultSet = {
    result.body match {
      case Void => new EmptyResultSet(executionInfo)
      case rows: Rows if rows.metaData.hasMorePages => new PaginatedResultSet(rows)
      case rows: Rows => new SinglePageResultSet(rows.content, executionInfo)
      case SetKeyspace(name) => empty //TODO:
      case SchemaChange(_, _, _)  => empty //TODO:
      case _ => throw new IllegalArgumentException(s"Unknown kind for result: [$result]")
    }
  }
}