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

package net.vangas.cassandra.event

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit}
import akka.util.{ByteString, ByteStringBuilder}
import net.vangas.cassandra.CassandraConstants._
import net.vangas.cassandra.message.TopologyChangeType._
import net.vangas.cassandra.message.StatusChangeType._
import net.vangas.cassandra.message.{StatusChangeEvent, TopologyChangeEvent}
import net.vangas.cassandra.{Serializable, VangasActorTestSupport, _}

class ServerEventListenerSpec extends TestKit(ActorSystem("ServerEventListenerSystem")) with VangasActorTestSupport {

  system.eventStream.subscribe(self, classOf[TopologyChangeEvent])
  system.eventStream.subscribe(self, classOf[StatusChangeEvent])

  val eventListener = TestActorRef(new ServerEventListener)

  describe("ServerEventListener") {
    it("should listen TopologyChange events") {
      val node1 = InetAddress.getByName("127.0.0.1")
      val node2 = InetAddress.getByName("127.0.0.2")
      eventListener ! event("TOPOLOGY_CHANGE", NEW_NODE.toString, node1)
      eventListener ! event("TOPOLOGY_CHANGE", REMOVED_NODE.toString, node2)

      expectMsg(TopologyChangeEvent(NEW_NODE, node1))
      expectMsg(TopologyChangeEvent(REMOVED_NODE, node2))
    }

    it("should listen StatusChange events") {
      val node1 = InetAddress.getByName("127.0.0.1")
      val node2 = InetAddress.getByName("127.0.0.2")
      eventListener ! event("STATUS_CHANGE", UP.toString, node1)
      eventListener ! event("STATUS_CHANGE", DOWN.toString, node2)

      expectMsg(StatusChangeEvent(UP, node1))
      expectMsg(StatusChangeEvent(DOWN, node2))
    }
  }

  private def event(eventType: String, changeType: String, node: InetAddress): ByteString = {
    val eventData = new ByteStringBuilder().putShort(eventType.length).append(ByteString.fromString(eventType))
    eventData.putShort(changeType.length).append(ByteString.fromString(changeType))
    eventData.append(Serializable(node).serialize())
    new ByteStringBuilder()
      .putByte(VERSION_FOR_V3)
      .putByte(FLAGS)
      .putShort(123)
      .putByte(EVENT)
      .putInt(111) //FrameLength is not used for now
      .append(eventData.result())
      .result()
  }

}
