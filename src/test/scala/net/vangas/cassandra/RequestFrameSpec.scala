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
import net.vangas.cassandra.CassandraConstants._
import org.scalatest.FunSpec
import org.scalatest.Matchers._

class RequestFrameSpec extends FunSpec {


  describe("Startup") {
    it("should create startup frame correctly") {
      val strtupFrame = RequestFrame(Header(VERSION_FOR_V3, FLAGS, 111, STARTUP), Startup).serialize
      val byteIterator = strtupFrame.iterator
      //HEADER
      byteIterator.getByte should be(VERSION_FOR_V3)
      byteIterator.getByte should be(FLAGS)
      byteIterator.getShort should be(111)
      byteIterator.getByte should be(STARTUP)
      //LENGTH of BODY
      byteIterator.getInt should be(22)
      //BODY
      byteIterator.getShort should be(1)
      byteIterator.getShort should be("CQL_VERSION".length)
      val cqlVersionStringArray = new Array[Byte]("CQL_VERSION".length)
      byteIterator.getBytes(cqlVersionStringArray)
      new String(cqlVersionStringArray) should be("CQL_VERSION")
      byteIterator.getShort should be("3.0.0".length)
      val cqlVersionArray = new Array[Byte]("3.0.0".length)
      byteIterator.getBytes(cqlVersionArray)
      new String(cqlVersionArray) should be("3.0.0")
    }
  }

}
