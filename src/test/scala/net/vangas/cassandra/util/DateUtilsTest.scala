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

import java.util.UUID
import org.joda.time.DateTime
import org.scalatest.FunSpec
import org.scalatest.Matchers._

class DateUtilsTest extends FunSpec {

  describe("DateUtils") {
    it("should convert timeuuid to datetime") {
      val dateTime = DateUtils.toDateTime(UUID.fromString("658c01d0-8472-11e4-b3db-df6007f92d61"))
      dateTime should be(new DateTime(2014, 12, 15, 16, 52, 39, 277))
    }

    it("should throw illegalargumentexception for non timeuuid") {
      intercept[IllegalArgumentException] {
        DateUtils.toDateTime(UUID.fromString("95e154fb-46ad-42df-8619-6b7725e099e2"))
      }
    }
  }

}
