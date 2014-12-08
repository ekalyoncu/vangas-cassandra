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

package net.vangas.cassandra.message

import org.scalatest.FunSpec
import org.scalatest.Matchers._
import net.vangas.cassandra.byteOrder
import akka.util.ByteStringBuilder

class PrepareSpec extends FunSpec {

  describe("Prepare") {
    it("should serialize") {
      val query = "select *"
      val data = new ByteStringBuilder().putInt(query.getBytes.length).putBytes(query.getBytes).result()
      Prepare(query).serialize should be(data)
    }
  }

}
