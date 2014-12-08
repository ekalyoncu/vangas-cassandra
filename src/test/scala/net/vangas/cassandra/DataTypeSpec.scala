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

import org.scalatest.FunSpec
import org.scalatest.Matchers._
import akka.util.{ByteStringBuilder, ByteString}
import java.nio.ByteBuffer
import org.joda.time.DateTime
import java.net.InetAddress

class DataTypeSpec extends FunSpec {

  describe("ASCII DataType") {
    it("should deserialize") {
      ASCII.deserialize(ByteString.fromString("test123")) should be("test123")
    }
  }

  describe("VARCHAR DataType") {
    it("should deserialize") {
      VARCHAR.deserialize(ByteString.fromString("test123_şüçğ")) should be("test123_şüçğ")
    }
  }

  describe("INT DataType") {
    it("should deserialize") {
      val intBytes = ByteBuffer.allocate(4).putInt(11).array()
      INT.deserialize(ByteString.fromArray(intBytes)) should be(11)
    }
  }

  describe("LONG DataType") {
    it("should deserialize") {
      val longBytes = ByteBuffer.allocate(8).putLong(22).array()
      LONG.deserialize(ByteString.fromArray(longBytes)) should be(22)
    }
  }

  describe("DECIMAL DataType") {
    def decimalToByeString(decimal: BigDecimal): ByteString = {
      new ByteStringBuilder()
        .putInt(decimal.scale)
        .putBytes(decimal.underlying().unscaledValue().toByteArray)
        .result()
    }

    it("should deserialize") {
      DECIMAL.deserialize(decimalToByeString(BigDecimal(1234567890))) should be(BigDecimal(1234567890))
    }
  }

  describe("BIGINT DataType") {
    it("should deserialize") {
      val bingIntBytes = BigInt(123456).toByteArray
      BIGINT.deserialize(ByteString.fromArray(bingIntBytes)) should be(BigInt(123456))
    }
  }

  describe("BOOLEAN DataType") {
    it("should deserialize") {
      BOOLEAN.deserialize(ByteString.fromArray(Array[Byte](0))) should be(false)
      BOOLEAN.deserialize(ByteString.fromArray(Array[Byte](1))) should be(true)
    }
  }

  describe("BLOB DataType") {
    it("should deserialize") {
      BLOB.deserialize(ByteString.fromString("test_blob_deser")) should be(ByteString.fromString("test_blob_deser"))
    }
  }
  
  describe("TIMESTAMP DataType") {
    def timeToByteString(time: DateTime) =
      ByteString.fromArray(ByteBuffer.allocate(8).putLong(time.getMillis).array())

    it("should deserialize") {
      val expectedTime = DateTime.now()
      TIMESTAMP.deserialize(timeToByteString(expectedTime)) should be(expectedTime)
    }
  }

  describe("UUID DataType") {
    def uuidToByteString(uuid: JUUID) =
      ByteString.fromArray(ByteBuffer
      .allocate(16)
      .putLong(0, uuid.getMostSignificantBits)
      .putLong(8, uuid.getLeastSignificantBits)
      .array())

    it("should deserialize") {
      val uuid = java.util.UUID.randomUUID()
      UUID.deserialize(uuidToByteString(uuid)) should be(uuid)
    }
  }

  describe("INET DataType") {
    it("should deserialze") {
      val address = InetAddress.getByName("192.168.1.2").getAddress
      INET.deserialize(ByteString.fromArray(address)) should be(InetAddress.getByName("192.168.1.2"))
    }
  }

  describe("LIST DataType") {
    it("should deserialize") {
      val data = new ByteStringBuilder()
        .putInt(2)
        .putInt("item1".getBytes.length).putBytes("item1".getBytes)
        .putInt("item2".getBytes.length).putBytes("item2".getBytes)
        .result()
      new LIST(VARCHAR).deserialize(data) should be(Seq("item1", "item2"))
    }
  }

  describe("SET DataType") {
    it("should deserialize") {
      val data = new ByteStringBuilder()
        .putInt(2)
        .putInt("item1".getBytes.length).putBytes("item1".getBytes)
        .putInt("item2".getBytes.length).putBytes("item2".getBytes)
        .result()
      new SET(VARCHAR).deserialize(data) should be(Set("item1", "item2"))
    }
  }

  describe("MAP DataType") {
    it("should deserialize") {
      val keyBytes = ByteBuffer.allocate(4).putInt(1).array()
      val data = new ByteStringBuilder()
        .putInt(1)
        .putInt(keyBytes.length).putBytes(keyBytes)
        .putInt("item1".getBytes.length).putBytes("item1".getBytes)
        .result()
      new MAP(INT, VARCHAR).deserialize(data) should be(Map(1 -> "item1"))
    }
  }


}
