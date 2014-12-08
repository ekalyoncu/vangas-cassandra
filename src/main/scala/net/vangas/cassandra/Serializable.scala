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

import akka.util.{ByteStringBuilder, ByteString}
import org.joda.time.DateTime
import java.net.InetAddress
import CassandraConstants.UTF_8
import java.nio.ByteBuffer

trait Serializable {
  def serialize(): ByteString
}

object Serializable {
  def apply(value: Any): Serializable = value match {
    case s: String => new StringSerializable(s)
    case i: Int  => new IntSerializable(i)
    case l: Long  => new LongSerializable(l)
    case f: Float  => new FloatSerializable(f)
    case d: Double  => new DoubleSerializable(d)
    case bi: BigInt  => new BigIntSerializable(bi)
    case bd: BigDecimal  => new BigDecimalSerializable(bd)
    case blob: Array[Byte]  => new BlobSerializable(blob)
    case bool: Boolean  => new BooleanSerializable(bool)
    case dt: DateTime  => new LongSerializable(dt.getMillis)
    case uuid: JUUID  => new UUIDSerializable(uuid)
    case inet: InetAddress  => new InetSerializable(inet)
    case seq: Seq[_] => new SeqSerializable(seq)
    case set: Set[_] => new SetSerializable(set)
    case map: Map[_, _] => new MapSerializable(map)
    case x => throw new IllegalArgumentException(s"Unsupported value type[${x.getClass}]")
  }
}

class StringSerializable(value: String) extends Serializable {
  def serialize(): ByteString = ByteString.fromArray(value.getBytes(UTF_8))
}

class IntSerializable(value: Int) extends Serializable {
  def serialize(): ByteString = ByteString.fromArray(ByteBuffer.allocate(4).putInt(value).array())
}

class LongSerializable(value: Long) extends Serializable {
  def serialize(): ByteString = ByteString.fromArray(ByteBuffer.allocate(8).putLong(value).array())
}

class FloatSerializable(value: Float) extends Serializable {
  def serialize(): ByteString = ByteString.fromArray(ByteBuffer.allocate(4).putFloat(value).array())
}

class DoubleSerializable(value: Double) extends Serializable {
  def serialize(): ByteString = ByteString.fromArray(ByteBuffer.allocate(8).putDouble(value).array())
}

class BigIntSerializable(value: BigInt) extends Serializable {
  def serialize(): ByteString = ByteString.fromArray(value.toByteArray)
}

class BigDecimalSerializable(value: BigDecimal) extends Serializable {
  def serialize(): ByteString = new ByteStringBuilder()
    .putInt(value.scale)
    .putBytes(value.underlying().unscaledValue().toByteArray)
    .result()
}

class BlobSerializable(value: Array[Byte]) extends Serializable {
  def serialize(): ByteString = ByteString.fromArray(value)
}

class BooleanSerializable(value: Boolean) extends Serializable {
  private[this] val TRUE = ByteString.fromArray(Array[Byte](1))
  private[this] val FALSE = ByteString.fromArray(Array[Byte](0))

  def serialize(): ByteString = if (value) TRUE else FALSE
}

class UUIDSerializable(value: JUUID) extends Serializable {
  def serialize(): ByteString = ByteString.fromArray(ByteBuffer
    .allocate(16)
    .putLong(0, value.getMostSignificantBits)
    .putLong(8, value.getLeastSignificantBits)
    .array())
}

class InetSerializable(value: InetAddress) extends Serializable {
  def serialize(): ByteString = ByteString.fromArray(value.getAddress)
}


class SeqSerializable(value: Seq[_]) extends CollectionSerializable(value)

class SetSerializable(value: Set[_]) extends CollectionSerializable(value)

abstract class CollectionSerializable(value: Iterable[_]) extends Serializable {
  def serialize(): ByteString = {
    val builder = new ByteStringBuilder()
    builder.putInt(value.size)
    value.foreach{ item =>
      val bytes = Serializable(item).serialize()
      builder.putInt(bytes.length).append(bytes)
    }
    builder.result()
  }
}

class MapSerializable(map: Map[_, _]) extends Serializable {
  def serialize(): ByteString = {
    val builder = new ByteStringBuilder()
    builder.putInt(map.size)
    map.foreach{ case (key, value) =>
      val keyBytes = Serializable(key).serialize()
      val valueBytes = Serializable(value).serialize()
      builder.putInt(keyBytes.length).append(keyBytes)
      builder.putInt(valueBytes.length).append(valueBytes)
    }
    builder.result()
  }
}