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

import scala.concurrent.{ExecutionContext, Future}
import net.vangas.cassandra._

/**
 * Session interface which implementations send requests to connections.
 * All operations here are asynchronous.
 *
 */
trait Session {

  /**
   * Prepares query for later execution.
   *
   * @param query CQL query
   * @return Future of PreparedStatement
   */
  def prepare(query: String)(implicit executor: ExecutionContext): Future[PreparedStatement]


  /**
   * Executes prepared statement
   *
   * @param statement Statement to be sent to cassandra node
   * @return Future of ResultSet
   */
  def execute(statement: Statement)(implicit executor: ExecutionContext): Future[ResultSet]

  /**
   * Executes CQL query
   *
   * @param query CQL query
   * @param params Variables to be bound in query
   * @return Future of ResultSet
   */
  def execute(query: String, params: Any*)(implicit executor: ExecutionContext): Future[ResultSet]

  /**
   * Closes this session. You cannot reuse closed session.
   */
  def close(): Future[Boolean]

}
