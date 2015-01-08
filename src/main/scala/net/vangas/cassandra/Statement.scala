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

package net.vangas.cassandra

import net.vangas.cassandra.message._
import net.vangas.cassandra.config.QueryConfig

trait Statement {
  def toRequestMessage(implicit queryConfig: QueryConfig): RequestMessage
}

case class PrepareStatement(query: String) extends Statement {
  override def toRequestMessage(implicit queryConfig: QueryConfig) = Prepare(query)
}

case class SimpleStatement(query: String, params: Seq[_] = Seq.empty) extends Statement {
  def toRequestMessage(implicit queryConfig: QueryConfig): RequestMessage = {
    Query(query, queryConfig.toQueryParameters(params))
  }
}

class PreparedStatement(queryAwarePrepared: ExPrepared) {
  def bind(params: Any*): BoundStatement = new BoundStatement(this, params)
  private[cassandra] def prepared(): ExPrepared = queryAwarePrepared
}

case class BoundStatement(preparedStatement: PreparedStatement, params: Seq[_] = Seq.empty) extends Statement {

  def originalQuery: String = preparedStatement.prepared().originalQuery

  def toRequestMessage(implicit queryConfig: QueryConfig): RequestMessage = {
    val queryId = preparedStatement.prepared().prepared.id
    Execute(queryId, queryConfig.toQueryParameters(params))
  }

}

object Statement {
  def apply(query: String, params: Any*): SimpleStatement = {
    new SimpleStatement(query, params)
  }
}