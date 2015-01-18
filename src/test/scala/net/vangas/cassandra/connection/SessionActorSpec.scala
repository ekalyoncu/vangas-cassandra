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

import java.net.InetSocketAddress

import akka.actor._
import akka.testkit.{TestProbe, TestActorRef, TestKit}
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra._
import scala.concurrent.duration._

class SessionActorSpec extends TestKit(ActorSystem("SessionActorSystem")) with VangasActorTestSupport {

  val connectionPoolManager = TestProbe()
  val requestLifeCycle = TestProbe()
  val sessionActor = TestActorRef(new SessionActor(1, "TEST_KS", Seq(), Configuration(), null) with MockSessionComponents)

  describe("SessionActor") {
    it("should send statement to requestlifecycle and response back to sender") {
      connectionPoolManager.expectMsg("Started")
      sessionActor ! Statement("query")
      requestLifeCycle.expectMsg("Started")
      val ctx = requestLifeCycle.receiveOne(100 milliseconds).asInstanceOf[RequestContext]
      ctx.statement should be(Statement("query"))
      ctx.respond(ResponseContext(Right("doesn't matter")))
      expectMsg(Right("doesn't matter"))
    }

    it("should stop cpmanager actor when itself dies") {
      DriverActorBridge(system).activateSession(1)
      connectionPoolManager.expectMsg("Started")
      watch(sessionActor)
      watch(connectionPoolManager.ref)
      sessionActor ! PoisonPill
      connectionPoolManager.expectMsg("Closed")
      connectionPoolManager.expectNoMsg()
      receiveOne(100 milliseconds).asInstanceOf[Terminated].actor should be(sessionActor)
      DriverActorBridge(system).isSessionClosed(1) should be(true)
    }

    it("should restart cpmanager when itself restarts") {
      DriverActorBridge(system).activateSession(1)
      connectionPoolManager.expectMsg("Started")
      sessionActor.underlying.mailbox.suspend()
      sessionActor.underlying.restart(new RuntimeException("~~TEST~~"))
      sessionActor.underlying.mailbox.resume()
      connectionPoolManager.expectMsg("Closed")
      connectionPoolManager.expectMsg("Started")
      DriverActorBridge(system).isSessionClosed(1) should be(false)
    }

    it("should stop itself when cpmanager is dead") {
      DriverActorBridge(system).activateSession(1)
      DriverActorBridge(system).isSessionClosed(1) should be(false)
      watch(sessionActor)
      connectionPoolManager.ref ! PoisonPill
      receiveOne(100 milliseconds).asInstanceOf[Terminated].actor should be(sessionActor)
      DriverActorBridge(system).isSessionClosed(1) should be(true)
    }

  }

  trait MockSessionComponents extends SessionComponents {

    override def createConnectionManager(sessionId: Int, keyspace: String, nodes: Seq[InetSocketAddress], config: Configuration): Actor = {
      new ForwardingActor(connectionPoolManager.ref)
    }

    override def createRequestLifecycle(loadBalancer: ActorRef, connectionPoolManager: ActorRef, config: Configuration): Actor = {
      new ForwardingActor(requestLifeCycle.ref)
    }
  }


}


