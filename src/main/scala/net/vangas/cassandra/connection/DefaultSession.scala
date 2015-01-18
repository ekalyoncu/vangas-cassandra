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

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import net.vangas.cassandra._
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.error.RequestError
import net.vangas.cassandra.exception.{QueryExecutionException, QueryPrepareException}
import net.vangas.cassandra.message._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * Session class which handles all requests for one keyspace.
 * Don't change keyspace (via 'USE' query) after session created.
 * If you need to query new keyspace, create new session.
 *
 * @param sessionId Session id
 * @param config Configuration
 */
private[connection] class DefaultSession(sessionId: Int,
                                         sessionActor: ActorRef,
                                         actorBridge: DriverActorBridge,
                                         config: Configuration) extends Session {

  private[this] val LOG = LoggerFactory.getLogger(classOf[DefaultSession])
  private[this] var closed = false

  def prepare(query: String)(implicit executor: ExecutionContext): Future[PreparedStatement] = {
    def doExecute(input: Unit): Future[PreparedStatement] = {
      execute[PreparedStatement](PrepareStatement(query), sessionActor) {
        case (prepared: ExPrepared, promise) => promise.success(new PreparedStatement(prepared))
        case (e: RequestError, promise) => promise.failure(new QueryPrepareException(e))
      }
    }

    validateSessionState flatMap doExecute
  }

  def execute(statement: Statement)(implicit executor: ExecutionContext): Future[ResultSet] = {
    def validateStatement(input: Unit): Future[Statement] = {
      statement match {
        case SimpleStatement(query, _) if query.trim.toUpperCase.startsWith("USE") =>
          val err = "USE statement is not supported in session. Please create new session to query different keyspace"
          Future.failed(new IllegalArgumentException(err))
        case _ => Future.successful(statement)
      }
    }

    def doExecute(statement: Statement): Future[ResultSet] = {
      execute[ResultSet](statement, sessionActor) {
        case (resultSet: ResultSet, promise) => promise.success(resultSet)
        case (e: RequestError, promise) => promise.failure(new QueryExecutionException(e))
      }
    }

    validateSessionState flatMap validateStatement flatMap doExecute
  }

  def execute(query: String, params: Any*)(implicit executor: ExecutionContext): Future[ResultSet] = {
    execute(SimpleStatement(query, params))
  }

  def close(): Future[Boolean] = {
    LOG.info("Closing session...")
    closed = true
    gracefulStop(sessionActor, 1 second)
  }

  private def execute[T](statement: Statement, requestLifeCycle: ActorRef)
                        (func: (Any, Promise[T]) => Unit)
                        (implicit executor: ExecutionContext): Future[T] = {
    implicit val timeout = Timeout(config.queryTimeout)
    val p = Promise[T]()
    (requestLifeCycle ? statement).mapTo[Either[RequestError, Any]].map {
      case Right(result) => func(result, p)
      case Left(err) => func(err, p)
    }
    p.future
  }

  private def validateSessionState(): Future[Unit] = {
    if (closed || actorBridge.isSessionClosed(sessionId)) {
      Future.failed(new IllegalStateException("Session is closed!"))
    } else {
      Future.successful[Unit]()
    }
  }
}

object DefaultSession {
  val sessionCounter = new AtomicInteger(0)
  val LOG = LoggerFactory.getLogger("DefaultSession")

  def apply(keyspace: String,
            nodes: Seq[InetSocketAddress],
            loadBalancer: ActorRef,
            config: Configuration)
           (implicit system: ActorSystem): Session = {

    val sessionId = sessionCounter.incrementAndGet()
    LOG.info(s"Creating Session-$sessionId for keyspace[$keyspace].")

    val sessionActor =
      system.actorOf(Props(new SessionActor(sessionId, keyspace, nodes, config, loadBalancer) with SessionComponents), s"Session-$sessionId")

    val actorBridge = DriverActorBridge(system)

    new DefaultSession(sessionId, sessionActor, actorBridge, config)
  }
}

private[connection] class SessionActor(sessionId: Int,
                                       keyspace: String,
                                       nodes: Seq[InetSocketAddress],
                                       config: Configuration,
                                       loadBalancer: ActorRef)
  extends Actor { this: SessionComponents =>

  val connectionPoolManager =
    context.actorOf(Props(createConnectionManager(sessionId, keyspace, nodes, config)), s"CPManager-$sessionId")

  def receive = {
    case statement: Statement =>
      val requester = sender()
      val requestLifeCycle = context.actorOf(Props(createRequestLifecycle(loadBalancer, connectionPoolManager, config)))
      requestLifeCycle ! RequestContext(statement, respond(requester))
  }

  private def respond(requester: ActorRef)(responseContext: ResponseContext): Unit = {
    requester ! responseContext.response
  }
}