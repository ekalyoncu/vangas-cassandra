/*
 * Copyright (C) 2015 Egemen Kalyoncu
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


import org.scalatest.FunSpec
import org.scalatest.Matchers._

class PreparedIdSpec extends FunSpec {

  describe("PreparedId") {
    it("should have correct hashcode") {
      new PreparedId("ID".getBytes).hashCode() should be(java.util.Arrays.hashCode("ID".getBytes))
    }

    it("should have correct equals") {
      new PreparedId("ID".getBytes).equals(null) should be(false)
      new PreparedId("ID".getBytes).equals("") should be(false)
      new PreparedId("ID".getBytes).equals(new PreparedId("ID2".getBytes)) should be(false)
      new PreparedId("ID".getBytes).equals(new PreparedId("ID".getBytes)) should be(true)
    }
  }

}
