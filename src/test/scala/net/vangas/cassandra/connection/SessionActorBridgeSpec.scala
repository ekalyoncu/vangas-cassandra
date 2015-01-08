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

package net.vangas.cassandra.connection

import akka.actor.{Terminated, ActorSystem}
import akka.testkit.{TestKit, TestProbe}
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.{RequestLifecycleStarted, VangasActorTestSupport}
import scala.concurrent.duration._

class SessionActorBridgeSpec extends TestKit(ActorSystem("SessionActorBridgeActorSystem")) with VangasActorTestSupport {

  val config = Configuration()
  val loadBalancer = TestProbe()
  val connectionPools = TestProbe()

  val sessionActorBridge = new SessionActorBridge(loadBalancer.ref, connectionPools.ref, config, true)

  describe("SessionActorBridge") {
    it("should create RequestLifecycle actor") {

      system.eventStream.subscribe(self, RequestLifecycleStarted.getClass)

      sessionActorBridge.createRequestLifecycle() should not be(null)
      sessionActorBridge.createRequestLifecycle() should not be(null)

      expectMsg(RequestLifecycleStarted)
      expectMsg(RequestLifecycleStarted)
    }

    it("should close session") {
      watch(connectionPools.ref)
      sessionActorBridge.closeSession()
      receiveOne(100 milliseconds).asInstanceOf[Terminated].actor should be(connectionPools.ref)

      loadBalancer.expectNoMsg()
    }
  }

}
