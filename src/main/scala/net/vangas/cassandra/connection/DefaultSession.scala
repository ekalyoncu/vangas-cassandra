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

import akka.util.Timeout
import akka.actor._
import akka.pattern._
import net.vangas.cassandra._
import net.vangas.cassandra.CassandraConstants.UNPREPARED
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.exception.{QueryPrepareException, QueryExecutionException}
import net.vangas.cassandra.message._
import scala.concurrent.{ExecutionContext, Promise, Future}
import scala.concurrent.duration._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
 * Session class which handles all requests for one keyspace.
 * Don't change keyspace (via 'USE' query) after session created.
 * If you need to query new keyspace, create new session.
 *
 * @param keyspace keyspace to be queried.
 * @param config Configuration
 * @param connectionRouter Internal actor to route requests to connections in round-robin fashion.
 */
final class DefaultSession private[connection](keyspace: String,
                                               config: Configuration,
                                               connectionRouter: ActorRef) extends Session {

  require(config != null, "Configuration object cannot be null!")
  require(config.queryConfig != null, "QueryConfig object cannot be null!")

  private[this] val LOG = LoggerFactory.getLogger(classOf[DefaultSession])

  private val queryTimeout = FiniteDuration(config.queryTimeout, SECONDS)

  def prepare(query: String)(implicit executor: ExecutionContext): Future[PreparedStatement] = {
    execute[PreparedStatement](Prepare(query)){
      case (prepared @ ExPrepared(_, _, node), p) =>
        p.success(new PreparedStatement(prepared))
        connectionRouter ! PrepareOnAllNodes(query, node)
      case (e: NodeAwareError, p) => p.failure(new QueryPrepareException(e.toString))
    }
  }

  def execute(boundStatement: BoundStatement)(implicit executor: ExecutionContext): Future[ResultSet] = {
    def executeBound(statement: BoundStatement, retryCount: Int = 0): Future[ResultSet] = {
      execute[ResultSet](statement.executeRequest(config.queryConfig)){
        case (result: Result, p) => p.success(ResultSet(result))
        case (NodeAwareError(Error(UNPREPARED, _), node), p) =>
          if (retryCount <= config.maxPrepareRetryCount) {
            val query = statement.originalQuery
            val params = boundStatement.params
            LOG.warn(s"Query[$query] is not prepared on host[$node]. Retrying [attempt $retryCount] to prepare and execute...")
            prepare(query) onComplete {
              case Success(prepared) =>
                LOG.debug(s"Query[$query] is now prepared on host[$node].")
                p completeWith executeBound(prepared.bind(params), retryCount + 1)
              case Failure(t) => p.failure(t)
            }
          }
        case (e: NodeAwareError, p) => p.failure(new QueryExecutionException(e.toString))
      }
    }
    executeBound(boundStatement)
  }

  def execute(query: String, params: Any*)(implicit executor: ExecutionContext): Future[ResultSet] = {
    execute[ResultSet](Query(query, config.queryConfig.toQueryParameters(params))){
      case (result: Result, p) => p.success(ResultSet(result))
      case (e: NodeAwareError, p) => p.failure(new QueryExecutionException(e.toString))
    }
  }

  def close(): Unit = {
    LOG.info("Closing session...")
    connectionRouter ! CloseRouter
  }

  private def execute[T](request: RequestMessage)
                        (func: (Any, Promise[T]) => Unit)
                        (implicit executor: ExecutionContext): Future[T] = {
    implicit val timeout = Timeout(queryTimeout)
    val p = Promise[T]()
    (connectionRouter ? request).map(func(_, p))
    p.future
  }

}

object DefaultSession {
  val sessionCounter = new AtomicInteger(0)

  def apply(keyspace: String, nodes: Seq[InetSocketAddress], config: Configuration)(implicit system: ActorSystem): Session = {
    val props = Props(new ConnectionRouter(keyspace, nodes, config) with ConnectionRouterComponents)
    val sessionId = sessionCounter.incrementAndGet()
    val connectionRouter = system.actorOf(props, s"connectionRouter-$sessionId")
    new DefaultSession(keyspace, config, connectionRouter)
  }
}