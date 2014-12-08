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

import akka.util.{ByteIterator, ByteString}
import net.vangas.cassandra.util.ByteUtils.readByteString
import net.vangas.cassandra.CassandraConstants._
import org.joda.time.DateTime
import java.net.InetAddress
import scala.collection.breakOut

abstract class DataType[T : Manifest] {
  def deserialize(data: ByteString): T

  def isTypeOf[U: Manifest] = {
    val thisType = implicitly[Manifest[T]].runtimeClass
    val thatType = implicitly[Manifest[U]].runtimeClass
    thisType == thatType
  }

  def notTypeOf[U: Manifest] = !isTypeOf[U]
}

object DataType {

  def apply(data: ByteIterator): DataType[_] = {
    data.getShort match {
      case TYPE_ASCII => ASCII
      case TYPE_VARCHAR => VARCHAR
      case TYPE_INT => INT
      case TYPE_BIGINT | TYPE_COUNTER => LONG
      case TYPE_VARINT => BIGINT
      case TYPE_DECIMAL => DECIMAL
      case TYPE_BLOB | TYPE_CUSTOM => BLOB
      case TYPE_BOOLEAN => BOOLEAN
      case TYPE_FLOAT => FLOAT
      case TYPE_DOUBLE => DOUBLE
      case TYPE_TIME_STAMP => TIMESTAMP
      case TYPE_UUID | TYPE_TIME_UUID => UUID
      case TYPE_INET => INET
      case TYPE_LIST => new LIST(DataType(data))
      case TYPE_SET => new SET(DataType(data))
      case TYPE_MAP => new MAP(DataType(data), DataType(data))
      case x => throw new IllegalArgumentException(s"Unsupported datatype id[$x]")
    }
  }
}


object ASCII extends DataType[String] {
  def deserialize(data: ByteString): String = data.decodeString(US_ASCII.name())
}

object VARCHAR extends DataType[String] {
  def deserialize(data: ByteString): String = data.decodeString(UTF_8.name())
}


object DOUBLE extends DataType[Double] {
  def deserialize(data: ByteString): Double = data.iterator.getDouble
}

object FLOAT extends  DataType[Float] {
  def deserialize(data: ByteString): Float = data.iterator.getFloat
}

object INT extends  DataType[Int] {
  def deserialize(data: ByteString): Int = data.iterator.getInt
}

object LONG extends DataType[Long] {
  def deserialize(data: ByteString): Long = data.iterator.getLong
}

object DECIMAL extends DataType[BigDecimal] {

  def deserialize(data: ByteString): BigDecimal = {
    val byteBuffer = data.toByteBuffer
    val scale = byteBuffer.getInt
    val bytes = new Array[Byte](byteBuffer.remaining())
    byteBuffer.get(bytes)
    BigDecimal(new java.math.BigInteger(bytes), scale)
  }
}

object BIGINT extends DataType[BigInt] {

  def deserialize(data: ByteString): BigInt = BigInt(data.iterator.toArray)
}

object BOOLEAN extends DataType[Boolean] {
  def deserialize(data: ByteString): Boolean = data.iterator.getByte != 0
}


object BLOB extends DataType[ByteString] {
  def deserialize(data: ByteString): ByteString = data
}

object TIMESTAMP extends DataType[DateTime] {
  def deserialize(data: ByteString): DateTime = new DateTime(LONG.deserialize(data))
}

object UUID extends DataType[JUUID] {
  def deserialize(data: ByteString): JUUID = {
    val iter = data.iterator
    new JUUID(iter.getLong, iter.getLong)
  }
}

object INET extends DataType[InetAddress] {
  def deserialize(data: ByteString): InetAddress = InetAddress.getByAddress(data.toArray)
}

class LIST(elementType: DataType[_]) extends DataType[Seq[_]] {

  def deserialize(data: ByteString): Seq[Any] = {
    val iter = data.iterator
    val len = iter.getInt
    0 until len map { _ =>
      //.get is safe because CQL doesn't support nulls in list
      readByteString(iter).map(elementType.deserialize).get
    }
  }
}

class SET(elementType: DataType[_]) extends DataType[Set[Any]] {

  def deserialize(data: ByteString): Set[Any] = {
    //TODO: fix code duplication
    val iter = data.iterator
    val len = iter.getInt
    (for(i <- 0 until len)
      yield readByteString(iter).map(elementType.deserialize).get
      )(breakOut)
  }
}


class MAP(val keyType: DataType[_], val valueType: DataType[_]) extends DataType[Map[Any, Any]] {

  def deserialize(data: ByteString): Map[Any, Any] = {
    val iter = data.iterator
    val len = iter.getInt
    (for(i <- 0 until len)
    yield {
      val key = readByteString(iter).map(keyType.deserialize).get
      val value = readByteString(iter).map(valueType.deserialize).get
      key -> value
    })(breakOut)
  }
}


