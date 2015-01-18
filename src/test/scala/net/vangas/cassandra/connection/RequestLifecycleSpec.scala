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

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Terminated}
import akka.pattern._
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import net.vangas.cassandra.CassandraConstants._
import net.vangas.cassandra._
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.error.{RequestErrorCode, RequestError}
import net.vangas.cassandra.message._
import scala.concurrent._
import scala.concurrent.duration._
import VangasTestHelpers._

class RequestLifecycleSpec extends TestKit(ActorSystem("RequestLifecycleSystem")) with VangasActorTestSupport {

  val response = TestProbe()
  val loadBalancer = TestProbe()
  val connectionPools = TestProbe()
  def createRequestLifecycle(queryTimeout: FiniteDuration = 10 seconds) =
    TestActorRef(new RequestLifecycle(loadBalancer.ref, connectionPools.ref, Configuration(queryTimeout = queryTimeout)))

  describe("RequestLifecycle") {
    it("should send request and get response") {
      val connection = TestProbe()
      val requestLifecycle = createRequestLifecycle()
      watch(requestLifecycle)
      val node1 = node(8888)
      requestLifecycle ! RequestContext(Statement("test_query"), respond())

      loadBalancer.expectMsg(CreateQueryPlan)
      loadBalancer.reply(QueryPlan(Iterator(node1)))
      connectionPools.expectMsg(GetConnectionFor(node1))
      connectionPools.reply(ConnectionReceived(connection.ref, node1))
      connection.expectMsg(Query("test_query", QueryParameters()))
      connection.reply(Result(Void))

      val resultSet = response.receiveOne(100 milliseconds).asInstanceOf[ResultSet]
      resultSet.executionInfo().triedNodes should be(Seq(node1))
      receiveOne(100 milliseconds).asInstanceOf[Terminated].actor should be(requestLifecycle)
    }

    it("should kill itself when there is ReceiveTimeout") {
      val requestLifecycle = createRequestLifecycle(50 milliseconds)
      watch(requestLifecycle)
      receiveOne(100 milliseconds).asInstanceOf[Terminated].actor should be(requestLifecycle)
    }

    it("should kill itself when there is ReceiveTimeout and actor is in handleRequest state") {
      val requestLifecycle = createRequestLifecycle(50 milliseconds)
      watch(requestLifecycle)
      requestLifecycle ! RequestContext(Statement("timed_out_query"), respond())
      loadBalancer.expectMsg(CreateQueryPlan)
      receiveOne(100 milliseconds).asInstanceOf[Terminated].actor should be(requestLifecycle)
    }

    it("should kill itself when there is ReceiveTimeout and actor is in successOrRetryAllNodes state") {
      val requestLifecycle = createRequestLifecycle(50 milliseconds)
      watch(requestLifecycle)
      requestLifecycle ! RequestContext(Statement("timed_out_query"), null)
      loadBalancer.expectMsg(CreateQueryPlan)
      loadBalancer.reply(QueryPlan(Iterator(node(8888))))

      receiveOne(100 milliseconds).asInstanceOf[Terminated].actor should be(requestLifecycle)
    }

    it("should kill itself and send error back to requester when there is no host left to query") {
      val requestLifecycle = createRequestLifecycle()
      watch(requestLifecycle)
      requestLifecycle ! RequestContext(Statement("timed_out_query"), respond())
      loadBalancer.expectMsg(CreateQueryPlan)
      loadBalancer.reply(QueryPlan(Iterator.empty))

      val err = "All hosts in queryplan are queried and none of them was successful."
      response.expectMsg(RequestError(RequestErrorCode.NO_HOST_AVAILABLE, err))
      receiveOne(100 milliseconds).asInstanceOf[Terminated].actor should be(requestLifecycle)
    }

    it("should try next host when connection is closed") {
      val requestLifecycle = createRequestLifecycle()
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      val node1 = node(8888)
      val node2 = node(9999)
      requestLifecycle ! RequestContext(Statement("retry_query"), respond())
      loadBalancer.expectMsg(CreateQueryPlan)
      loadBalancer.reply(QueryPlan(Iterator(node1, node2)))
      connectionPools.expectMsg(GetConnectionFor(node1))
      connectionPools.reply(ConnectionReceived(connection1.ref, node1))
      connection1.ref ! PoisonPill
      connectionPools.expectMsg(GetConnectionFor(node2))
      connectionPools.reply(ConnectionReceived(connection2.ref, node2))

      connection2.expectMsg(Query("retry_query", QueryParameters()))
      connection2.reply(Result(Void))

      val resultSet = response.receiveOne(100 milliseconds).asInstanceOf[ResultSet]
      resultSet.executionInfo().triedNodes should be(Seq(node1, node2))
    }

    it("should try next host for server-side errors") {
      val requestLifecycle = createRequestLifecycle()
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      val node1 = node(8888)
      val node2 = node(9999)
      requestLifecycle ! RequestContext(Statement("retry_query_2"), respond())
      loadBalancer.expectMsg(CreateQueryPlan)
      loadBalancer.reply(QueryPlan(Iterator(node1, node2)))
      connectionPools.expectMsg(GetConnectionFor(node1))

      connectionPools.reply(ConnectionReceived(connection1.ref, node1))
      requestLifecycle ! NodeAwareError(Error(OVERLOADED, "~TEST1~"), node1)

      connectionPools.expectMsg(GetConnectionFor(node2))
      connectionPools.reply(ConnectionReceived(connection2.ref, node1))

      //killing previous connection should not trigger Terminated msg
      //because we should unwatch previous connections
      connection1.ref ! PoisonPill

      connection2.expectMsg(Query("retry_query_2", QueryParameters()))
      connection2.reply(Result(Void))

      response.receiveOne(100 milliseconds) shouldBe a[EmptyResultSet]
    }

    it("should try next host when there is no connection for current host") {
      val requestLifecycle = createRequestLifecycle()
      val connection2 = TestProbe()
      val node1 = node(8888)
      val node2 = node(9999)
      requestLifecycle ! RequestContext(Statement("retry_query_3"), respond())
      loadBalancer.expectMsg(CreateQueryPlan)
      loadBalancer.reply(QueryPlan(Iterator(node1, node2)))
      connectionPools.expectMsg(GetConnectionFor(node1))

      connectionPools.reply(NoConnectionFor(node1))

      connectionPools.expectMsg(GetConnectionFor(node2))
      connectionPools.reply(ConnectionReceived(connection2.ref, node2))

      connection2.expectMsg(Query("retry_query_3", QueryParameters()))
      connection2.reply(Result(Void))

      val resultSet = response.receiveOne(100 milliseconds).asInstanceOf[ResultSet]
      resultSet.executionInfo().triedNodes should be(Seq(node2))
    }

    it("should retry next host when there is the connection reaches its max streamid") {
      val requestLifecycle = createRequestLifecycle()
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      val node1 = node(8888)
      val node2 = node(9999)
      requestLifecycle ! RequestContext(Statement("retry_query_4"), respond())
      loadBalancer.expectMsg(CreateQueryPlan)
      loadBalancer.reply(QueryPlan(Iterator(node1, node2)))
      connectionPools.expectMsg(GetConnectionFor(node1))
      connectionPools.reply(ConnectionReceived(connection1.ref, node1))
      connection1.expectMsg(Query("retry_query_4", QueryParameters()))
      requestLifecycle ! MaxStreamIdReached(connection1.ref)

      connectionPools.expectMsg(GetConnectionFor(node2))
      connectionPools.reply(ConnectionReceived(connection2.ref, node2))

      //killing previous connection should not trigger Terminated msg
      //because we should unwatch previous connections
      connection1.ref ! PoisonPill

      connection2.expectMsg(Query("retry_query_4", QueryParameters()))
      connection2.reply(Result(Void))

      response.receiveOne(100 milliseconds) shouldBe a[EmptyResultSet]
    }

    it("should retry next host when currentConnection is defunct") {
      val requestLifecycle = createRequestLifecycle()
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      val node1 = node(8888)
      val node2 = node(9999)
      requestLifecycle ! RequestContext(Statement("retry_query_5"), respond())
      loadBalancer.expectMsg(CreateQueryPlan)
      loadBalancer.reply(QueryPlan(Iterator(node1, node2)))
      connectionPools.expectMsg(GetConnectionFor(node1))
      connectionPools.reply(ConnectionReceived(connection1.ref, node1))
      connection1.expectMsg(Query("retry_query_5", QueryParameters()))

      system.eventStream.publish(ConnectionDefunct(connection1.ref, node1))

      connectionPools.expectMsg(GetConnectionFor(node2))
      connectionPools.reply(ConnectionReceived(connection2.ref, node2))

      connection2.expectMsg(Query("retry_query_5", QueryParameters()))
      connection2.reply(Result(Void))

      response.receiveOne(100 milliseconds) shouldBe a[EmptyResultSet]
    }

    it("should not retry next host when other connection is defunct") {
      val otherConnection = TestProbe()
      val connection1 = TestProbe()
      val node1 = node(8888)

      val requestLifecycle = createRequestLifecycle()
      requestLifecycle ! RequestContext(Statement("retry_query_6"), respond())
      loadBalancer.expectMsg(CreateQueryPlan)
      loadBalancer.reply(QueryPlan(Iterator(node1)))

      connectionPools.expectMsg(GetConnectionFor(node1))
      connectionPools.reply(ConnectionReceived(connection1.ref, node1))
      connection1.expectMsg(Query("retry_query_6", QueryParameters()))

      system.eventStream.publish(ConnectionDefunct(otherConnection.ref, node1))

      connectionPools.expectNoMsg()

      response.expectNoMsg()
    }

    it("should return error back to requester for non-server errors") {
      val requestLifecycle = createRequestLifecycle()
      watch(requestLifecycle)
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      val node1 = node(8888)
      val node2 = node(9999)
      requestLifecycle ! RequestContext(Statement("query_with_error"), respond())
      loadBalancer.expectMsg(CreateQueryPlan)
      loadBalancer.reply(QueryPlan(Iterator(node1, node2)))
      connectionPools.expectMsg(GetConnectionFor(node1))

      connectionPools.reply(ConnectionReceived(connection1.ref, node1))
      requestLifecycle ! NodeAwareError(Error(INVALID_QUERY, "~TEST2~"), node1)
      connectionPools.expectNoMsg()

      connection1.expectMsg(Query("query_with_error", QueryParameters()))
      connection2.expectNoMsg()

      response.receiveOne(100 milliseconds) should be(RequestError(RequestErrorCode.INVALID_QUERY, "~TEST2~"))
      receiveOne(100 milliseconds).asInstanceOf[Terminated].actor should be(requestLifecycle)
    }

    it("should prepare on all nodes") {
      val requestLifecycle = createRequestLifecycle()
      watch(requestLifecycle)
      val underlyingActor = requestLifecycle.underlyingActor
      underlyingActor.context.become(new underlyingActor.ReadyForResponse(RequestContext(null, respond()), null).receive)
      val node1 = node(1111)
      requestLifecycle ! ExPrepared(null, "prepared_query", node1)
      response.expectMsg(ExPrepared(null, "prepared_query", node1))
      connectionPools.expectMsg(PrepareOnAllNodes(Prepare("prepared_query"), node1))
      receiveOne(100 milliseconds).asInstanceOf[Terminated].actor should be(requestLifecycle)
    }

    it("should not prepare query when error is unprepared and original request is not BoundStatement") {
      val requestLifecycle = createRequestLifecycle()
      val underlyingActor = requestLifecycle.underlyingActor
      watch(requestLifecycle)
      val statement = SimpleStatement("simple_query")
      requestLifecycle ! statement
      underlyingActor.context.become(
        new underlyingActor.ReadyForResponse(RequestContext(statement, respond()), Iterator(node(1111))).receive
      )
      requestLifecycle ! NodeAwareError(Error(UNPREPARED, "~TEST~"), node(1111))

      val err = "Error is UNPREPARED but statement is not BoundStatement"
      response.expectMsg(RequestError(RequestErrorCode.UNPREPARED_WITH_INCONSISTENT_STATEMENT, err))
      receiveOne(100 milliseconds).asInstanceOf[Terminated].actor should be(requestLifecycle)
    }

    it("should prepare unprepared query on failed node and send request to the next node") {
      val requestLifecycle = createRequestLifecycle()
      val connection1 = TestProbe()
      val connection2 = TestProbe()
      val node1 = node(8888)
      val node2 = node(9999)
      val prepared = new Prepared(new PreparedId("ID".getBytes), null, null)
      val statement = BoundStatement(new PreparedStatement(ExPrepared(prepared, "unprepared_query", node1)))

      requestLifecycle ! RequestContext(statement, respond())
      loadBalancer.expectMsg(CreateQueryPlan)
      loadBalancer.reply(QueryPlan(Iterator(node1, node2)))
      connectionPools.expectMsg(GetConnectionFor(node1))
      connectionPools.reply(ConnectionReceived(connection1.ref, node1))

      connection1.expectMsg(Execute(new PreparedId("ID".getBytes), QueryParameters()))

      requestLifecycle ! NodeAwareError(Error(UNPREPARED, "~UNPREPARED_IN_NODE1~"), node1)

      connection1.expectMsg(Prepare("unprepared_query"))

      connectionPools.expectMsg(GetConnectionFor(node2))
      connectionPools.reply(ConnectionReceived(connection2.ref, node2))

      connection2.expectMsg(Execute(new PreparedId("ID".getBytes), QueryParameters()))
    }
  }

  private def respond(requester: ActorRef = response.ref)(ctx: ResponseContext): Unit =
    ctx.response.fold(err => requester ! err, result => requester ! result)

}
