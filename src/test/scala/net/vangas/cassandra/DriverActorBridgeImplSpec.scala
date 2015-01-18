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

package net.vangas.cassandra

import org.scalatest.FunSpec
import org.scalatest.Matchers._
import scala.collection.JavaConverters._

class DriverActorBridgeImplSpec extends FunSpec {

  val bridge = new DriverActorBridgeImpl()

  describe("DriverActorBridgeImpl") {
    it("should activate session") {
      bridge.activateSession(1)
      bridge.activeSessions.asScala should be(Map(1 -> true))
      bridge.isSessionClosed(1) should be(false)
    }

    it("should close session") {
      bridge.activateSession(1)
      bridge.isSessionClosed(1) should be(false)
      bridge.activateSession(2)
      bridge.isSessionClosed(2) should be(false)
      bridge.activeSessions.asScala should be(Map(1 -> true, 2 -> true))
      bridge.closeSession(1)
      bridge.isSessionClosed(1) should be(true)
      bridge.activeSessions.asScala should be(Map(2 -> true))
      bridge.closeSession(2)
      bridge.isSessionClosed(2) should be(true)
      bridge.activeSessions.asScala should be(Map.empty)
    }

  }

}
