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

package net.vangas.cassandra.connection

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import akka.util.{ByteString, ByteStringBuilder}
import net.vangas.cassandra.CassandraConstants._
import net.vangas.cassandra.VangasTestHelpers._
import net.vangas.cassandra.message.StatusChangeType._
import net.vangas.cassandra.message.TopologyChangeType._
import net.vangas.cassandra.message.{ExPrepared, _}
import net.vangas.cassandra.{Header, RequestStream, byteOrder, _}
import org.joda.time.{DateTime, DateTimeUtils}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar._
import scala.concurrent.duration._

class ResponseHandlerSpec extends TestKit(ActorSystem("ResponseHandlerSpec"))
  with VangasActorTestSupport with BeforeAndAfter {

  val header = mock[Header]
  val factory = mock[Factory[ByteString, ResponseFrame]]
  val authenticationProbe = TestProbe()
  val responseHandler = TestActorRef(new ResponseHandler(node(1111), factory))
  val fixedTime = DateTime.now

  before {
    DateTimeUtils.setCurrentMillisFixed(fixedTime.getMillis)
  }

  after {
    DateTimeUtils.setCurrentMillisSystem()
  }

  describe("ResponseHandler") {

    it("should handle ready response") {
      val res = ByteString.fromString("READY")
      when(factory.apply(res)).thenReturn(ResponseFrame(null, Ready))
      responseHandler ! ReceivedData(res, RequestStream(self, null))
      expectMsg(Ready)
    }

    it("should handle rows result") {
      val requester = TestProbe()
      val res = ByteString.fromString("RESULT_ROW")
      val rows = Rows(null, 2, IndexedSeq(new RowImpl(null, null)))
      when(factory.apply(res)).thenReturn(ResponseFrame(null, Result(rows)))
      responseHandler ! ReceivedData(res, RequestStream(requester.ref, null))
      requester.expectMsg(Result(rows))
    }

    it("should handle prepared response") {
      val requester = TestProbe()
      val res = ByteString.fromString("PREPARED")
      val prepared = Prepared(new PreparedId("id".getBytes), null, null)
      when(factory.apply(res)).thenReturn(ResponseFrame(null, Result(prepared)))
      responseHandler ! ReceivedData(res, RequestStream(requester.ref, Prepare("QUERY")))
      requester.expectMsg(ExPrepared(prepared, "QUERY", node(1111)))
    }

    it("should handle error response") {
      val res = ByteString.fromString("ERROR")
      when(factory.apply(res)).thenReturn(ResponseFrame(null, Error(111, "error_msg")))
      responseHandler ! ReceivedData(res, RequestStream(self, null))
      expectMsg(NodeAwareError(Error(111, "error_msg"), node(1111)))
      expectNoMsg()
    }

    it("should listen TopologyChange removed_node events") {
      system.eventStream.subscribe(self, classOf[TopologyChangeEvent])
      val responseHandler = TestActorRef(new ResponseHandler(null))
      val node1 = InetAddress.getByName("127.0.0.1")
      responseHandler ! event("TOPOLOGY_CHANGE", REMOVED_NODE.toString, node1)

      expectMsg(TopologyChangeEvent(REMOVED_NODE, node1))
      system.eventStream.unsubscribe(self)
    }

    it("should listen StatusChange down events") {
      system.eventStream.subscribe(self, classOf[StatusChangeEvent])
      val responseHandler = TestActorRef(new ResponseHandler(null))
      val node1 = InetAddress.getByName("127.0.0.1")
      responseHandler ! event("STATUS_CHANGE", DOWN.toString, node1)

      expectMsg(StatusChangeEvent(DOWN, node1))
      system.eventStream.unsubscribe(self)
    }

    it("should fire node up events after 1 second") {
      system.eventStream.subscribe(self, classOf[StatusChangeEvent])
      system.eventStream.subscribe(self, classOf[TopologyChangeEvent])
      val responseHandler = TestActorRef(new ResponseHandler(null))
      val node1 = InetAddress.getByName("127.0.0.1")
      val node2 = InetAddress.getByName("127.0.0.2")

      responseHandler ! event("STATUS_CHANGE", UP.toString, node1)

      expectNoMsg(900 milliseconds)
      expectMsg(StatusChangeEvent(UP, node1))

      responseHandler ! event("TOPOLOGY_CHANGE", NEW_NODE.toString, node2)

      expectNoMsg(900 milliseconds)
      expectMsg(TopologyChangeEvent(NEW_NODE, node2))
      system.eventStream.unsubscribe(self)
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

