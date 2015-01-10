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

import akka.util.ByteStringBuilder
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import net.vangas.cassandra._
import net.vangas.cassandra.util.ByteUtils.writeString

class RegisterForEventsSpec extends FunSpec {

  describe("RegisterForEvent") {
    it("should serialize register request") {
      val eventTypes = Seq("TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE")
      val registerData = RegisterForEvents(eventTypes).serialize
      val expected = new ByteStringBuilder()
      expected.putShort(3)
      eventTypes.foreach(e => expected.append(writeString(e)))
      registerData should be(expected.result())
    }

    it("should not accept invalid event type") {
      intercept[IllegalArgumentException]{
        RegisterForEvents(Seq("XXXX")).serialize
      }

      intercept[IllegalArgumentException]{
        RegisterForEvents(Seq("TOPOLOGY_CHANGE", "XXXX")).serialize
      }
    }

  }

}
