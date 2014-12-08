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

class SerializableSpec  extends FunSpec {

  describe("Serializable") {
    it("should serialize String") {
      Serializable("test123_şüçğ_serialize").serialize() should be (ByteString.fromString("test123_şüçğ_serialize"))
    }

    it("should serialize Int") {
      val intBytes = ByteBuffer.allocate(4).putInt(12345).array()
      Serializable(12345).serialize() should be(ByteString.fromArray(intBytes))
    }

    it("should serialize Long") {
      val longBytes = ByteBuffer.allocate(8).putLong(123456).array()
      Serializable(123456L).serialize() should be (ByteString.fromArray(longBytes))
    }

    it("should serialize BigDecimal") {
      def decimalToByeString(decimal: BigDecimal): ByteString = {
        new ByteStringBuilder()
          .putInt(decimal.scale)
          .putBytes(decimal.underlying().unscaledValue().toByteArray)
          .result()
      }

      val decimal = BigDecimal(1234567890.123)
      Serializable(decimal).serialize() should be (decimalToByeString(decimal))
    }

    it("should serialize BigInt") {
      Serializable(BigInt(12345)).serialize() should be (ByteString.fromArray(BigInt(12345).toByteArray))
    }


    it("should serialize Boolean") {
      Serializable(false).serialize() should be(ByteString.fromArray(Array[Byte](0)))
      Serializable(true).serialize() should be(ByteString.fromArray(Array[Byte](1)))
    }

    it("should serialize Blob") {
      Serializable("test_blob_ser".getBytes).serialize() should be(ByteString.fromString("test_blob_ser"))
    }

    it("should serialize Date") {
      def timeToByteString(time: DateTime) =
        ByteString.fromArray(ByteBuffer.allocate(8).putLong(time.getMillis).array())

      val now = DateTime.now()
      Serializable(now).serialize() should be (timeToByteString(now))
    }

    it("should serialize UUID") {
      def uuidToByteString(uuid: JUUID) =
        ByteString.fromArray(ByteBuffer
          .allocate(16)
          .putLong(0, uuid.getMostSignificantBits)
          .putLong(8, uuid.getLeastSignificantBits)
          .array())

      val uuid = java.util.UUID.randomUUID()
      Serializable(uuid).serialize() should be(uuidToByteString(uuid))
    }

    it("should serialize Inet") {
      val address = InetAddress.getByName("192.168.1.1")
      Serializable(address).serialize() should be(ByteString.fromArray(address.getAddress))
    }

    it("should serialize empty List") {
      val data = new ByteStringBuilder().putInt(0).result()
      Serializable(Seq.empty[Int]).serialize() should be(data)
    }

    it("should serialize List") {
      def intBytes(i: Int) = ByteBuffer.allocate(4).putInt(i).array()
      val data = new ByteStringBuilder()
        .putInt(3)
        .putInt(intBytes(1).length).putBytes(intBytes(1))
        .putInt(intBytes(2).length).putBytes(intBytes(2))
        .putInt(intBytes(3).length).putBytes(intBytes(3))
        .result()
      Serializable(Seq(1,2,3)).serialize() should be(data)
    }

    it("should serialize empty Set") {
      val data = new ByteStringBuilder().putInt(0).result()
      Serializable(Set.empty[String]).serialize() should be(data)
    }

    it("should serialize Set") {
      val data = new ByteStringBuilder()
        .putInt(2)
        .putInt("aaa".length).putBytes("aaa".getBytes)
        .putInt("bbb".length).putBytes("bbb".getBytes)
        .result()
      Serializable(Set("aaa", "bbb")).serialize() should be(data)
    }

    it("should serialize empty Map") {
      val data = new ByteStringBuilder().putInt(0).result()
      Serializable(Map.empty[String, String]).serialize() should be(data)
    }

    it("should serialize Map") {
      val data = new ByteStringBuilder()
        .putInt(1)
        .putInt("key".length).putBytes("key".getBytes)
        .putInt("value".length).putBytes("value".getBytes)
        .result()
      Serializable(Map("key" -> "value")).serialize() should be(data)
    }
  }

}
