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

import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * Session class which handles all requests for one keyspace.
 * Don't change keyspace (via 'USE' query) after session created.
 * If you need to query new keyspace, create new session.
 *
 * @param sessionId Session id
 * @param keyspace keyspace to be queried.
 * @param config Configuration
 * @param sessionActorBridge Interface between session and ActorSystem
 */
final class DefaultSession private[connection](sessionId: Int,
                                               keyspace: String,
                                               config: Configuration,
                                               sessionActorBridge: SessionActorBridge) extends Session {

  require(config != null, "Configuration object cannot be null!")
  require(config.queryConfig != null, "QueryConfig object cannot be null!")

  private[this] val LOG = LoggerFactory.getLogger(classOf[DefaultSession])
  private[this] var closed = false

  def prepare(query: String)(implicit executor: ExecutionContext): Future[PreparedStatement] = {
    if (closed) {
      return Future.failed(new IllegalStateException("Session is closed!"))
    }
    val requestLifeCycle = sessionActorBridge.createRequestLifecycle()

    execute[PreparedStatement](PrepareStatement(query), requestLifeCycle) {
      case (prepared: ExPrepared, p) => p.success(new PreparedStatement(prepared))
      case (e: RequestError, p) => p.failure(new QueryPrepareException(e))
    }
  }

  def execute(statement: Statement)(implicit executor: ExecutionContext): Future[ResultSet] = {
    def validateStatement(): Future[Statement] = {
      statement match {
        case SimpleStatement(query, _) if query.trim.toUpperCase.startsWith("USE") =>
          val err = "USE statement is not supported in session. Please create new session to query different keyspace"
          Future.failed(new IllegalArgumentException(err))
        case _ => Future.successful(statement)
      }
    }

    def doExecute(statement: Statement): Future[ResultSet] = {
      val requestLifeCycle = sessionActorBridge.createRequestLifecycle()
      execute[ResultSet](statement, requestLifeCycle) {
        case (resultSet: ResultSet, p) => p.success(resultSet)
        case (e: RequestError, p) => p.failure(new QueryExecutionException(e))
      }
    }

    if (closed) {
      return Future.failed(new IllegalStateException("Session is closed!"))
    }
    validateStatement flatMap doExecute
  }

  def execute(query: String, params: Any*)(implicit executor: ExecutionContext): Future[ResultSet] = {
    execute(SimpleStatement(query, params))
  }

  def close(): Future[Boolean] = {
    LOG.info("Closing session...")
    closed = true
    sessionActorBridge.closeSession()
  }

  private def execute[T](statement: Statement, requestLifeCycle: ActorRef)
                        (func: (Any, Promise[T]) => Unit)
                        (implicit executor: ExecutionContext): Future[T] = {
    implicit val timeout = Timeout(config.queryTimeout)
    val p = Promise[T]()
    (requestLifeCycle ? statement).map(func(_, p))
    p.future
  }

}

object DefaultSession {
  val sessionCounter = new AtomicInteger(0)
  val LOG = LoggerFactory.getLogger("DefaultSession")

  def apply(keyspace: String,
            nodes: Seq[InetSocketAddress],
            loadBalancer: ActorRef,
            config: Configuration)(implicit system: ActorSystem): Session = {
    val sessionId = sessionCounter.incrementAndGet()
    LOG.info(s"Creating session-$sessionId for keyspace[$keyspace].")

    val connectionPools = system.actorOf(
      Props(new ConnectionPools(keyspace, nodes, config) with ConnectionPoolsComponents),
      s"ConnectionPools-$sessionId"
    )
    new DefaultSession(sessionId ,keyspace, config, new SessionActorBridge(loadBalancer, connectionPools, config))
  }
}