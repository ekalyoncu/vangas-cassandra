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

import net.vangas.cassandra.message._
import org.scalatest.{OneInstancePerTest, BeforeAndAfterAll, BeforeAndAfter, FunSpecLike}
import org.mockito.Mockito.when
import akka.testkit.{TestProbe, TestActorRef, ImplicitSender, TestKit}
import akka.actor.{Actor, ActorSystem}
import akka.util.ByteString
import scala.concurrent.duration._
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar._
import org.joda.time.{DateTime, DateTimeUtils}
import java.net.InetSocketAddress
import net.vangas.cassandra._
import net.vangas.cassandra.Header
import akka.io.Tcp.Connected
import net.vangas.cassandra.RequestStream
import net.vangas.cassandra.message.ExPrepared
import net.vangas.cassandra.message.Authenticate

class ResponseHandlerSpec extends TestKit(ActorSystem("ResponseHandlerSpec"))
  with FunSpecLike with ImplicitSender with BeforeAndAfter with BeforeAndAfterAll with OneInstancePerTest {

  val header = mock[Header]
  val factory = mock[Factory[ByteString, ResponseFrame]]
  val authenticationProbe = TestProbe()
  val responseHandler = TestActorRef(new ResponseHandler(factory) with MockResponseHandlerComponents)
  val fixedTime = DateTime.now

  override def afterAll() {
    system.shutdown()
  }

  before {
    DateTimeUtils.setCurrentMillisFixed(fixedTime.getMillis)
  }

  after {
    DateTimeUtils.setCurrentMillisSystem()
  }

  describe("ResponseHandler") {
    it("should register cassandra connection") {
      responseHandler ! Connected(new InetSocketAddress("localhost", 9042), null)
      responseHandler.underlyingActor.node should be (new InetSocketAddress("localhost", 9042))
      responseHandler.underlyingActor.cassandraConnection should be (self)
    }

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
      responseHandler.underlyingActor.node = new InetSocketAddress("localhost", 1234)
      val requester = TestProbe()
      val res = ByteString.fromString("PREPARED")
      val prepared = Prepared(new PreparedId("id".getBytes), null, null)
      when(factory.apply(res)).thenReturn(ResponseFrame(null, Result(prepared)))
      responseHandler ! ReceivedData(res, RequestStream(requester.ref, Prepare("QUERY")))
      requester.expectMsg(ExPrepared(prepared, "QUERY", new InetSocketAddress("localhost", 1234)))
    }

    it("should handle error response") {
      responseHandler.underlyingActor.node = new InetSocketAddress("localhost", 1234)
      val res = ByteString.fromString("ERROR")
      when(factory.apply(res)).thenReturn(ResponseFrame(null, Error(111, "error_msg")))
      responseHandler ! ReceivedData(res, RequestStream(self, null))
      expectMsg(NodeAwareError(Error(111, "error_msg"), new InetSocketAddress("localhost", 1234)))
      expectNoMsg()
    }

    it("should handle authenticate response") {
      val res = ByteString.fromString("AUTH")
      when(factory.apply(res)).thenReturn(ResponseFrame(null, Authenticate(ByteString.fromString("token"))))
      responseHandler ! ReceivedData(res, RequestStream(self, null))
      authenticationProbe.expectMsg(2 seconds, Authenticate(ByteString.fromString("token")))
    }
  }

  trait MockResponseHandlerComponents extends ResponseHandlerComponents {
    override def createAuthentication: Actor = new ForwardingActor(authenticationProbe.ref)
  }
  
}

