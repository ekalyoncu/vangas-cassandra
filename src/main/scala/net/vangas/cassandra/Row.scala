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

import akka.util.ByteString
import net.vangas.cassandra.exception.InvalidColumnTypeException

trait Row {

  /**
   * Checks if column has data
   * @param columnIndex zero based column index
   * @return
   */
  def isNull(columnIndex: Int): Boolean

  /**
   * Gets column value if it's not null otherwise null
   * @param columnIndex zero based column index
   * @tparam T Type of column value
   * @return Column value
   */
  def getOrNull[T >: Null : Manifest ](columnIndex: Int): T

  /**
   * Gets column value as option
   * @param columnIndex zero based column index
   * @tparam T Type of column value
   * @return Column value
   */
  def get[T: Manifest](columnIndex: Int): Option[T]
}


class RowImpl(metaData: MetaData, columns: IndexedSeq[Option[ByteString]]) extends Row {

  def isNull(columnIndex: Int): Boolean = columns(columnIndex).isEmpty

  def getOrNull[T >: Null : Manifest](columnIndex: Int): T = get[T](columnIndex).orNull

  def get[T: Manifest](columnIndex: Int): Option[T] = {
    columns(columnIndex).map { data =>
      readColumn[T](columnIndex, metaData.columns(columnIndex).dataType, data)
    }
  }

  private def readColumn[T: Manifest](columnIndex: Int, dataType: DataType[_], data: ByteString): T = {
    if (dataType.notTypeOf[T]) {
      throw new InvalidColumnTypeException(s"Column $columnIndex has type ${dataType.getClass}.")
    }
    dataType.deserialize(data).asInstanceOf[T]
  }
}
