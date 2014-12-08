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

package net.vangas.cassandra.util

import akka.util.{ByteStringBuilder, ByteString, ByteIterator}
import net.vangas.cassandra.{Serializable, byteOrder}
import net.vangas.cassandra.CassandraConstants.UTF_8

object ByteUtils {

  def readString(data: ByteIterator): String = {
    val len = data.getShort
    val bytes = new Array[Byte](len)
    data.getBytes(bytes)
    new String(bytes, UTF_8)
  }

  def readBytes(data: ByteIterator): Option[Array[Byte]] = {
    val len = data.getInt
    if (len < 0) {
      None
    } else {
      val bytes = new Array[Byte](len)
      data.getBytes(bytes)
      Some(bytes)
    }
  }

  def readShortBytes(data: ByteIterator): Option[Array[Byte]] = {
    val len = data.getShort
    if (len < 0) {
      None
    } else {
      val bytes = new Array[Byte](len)
      data.getBytes(bytes)
      Some(bytes)
    }
  }

  def readByteString(data: ByteIterator): Option[ByteString] = {
    readBytes(data).map(ByteString.fromArray)
  }

  def valuesToByteString(values: Any*): ByteString = {
    val builder = new ByteStringBuilder()
    builder.putShort(values.length)
    values.foreach { value =>
      val byteString = Serializable(value).serialize()
      builder.putInt(byteString.length).append(byteString)
    }
    builder.result()
  }

}
