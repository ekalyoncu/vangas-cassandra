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

import net.vangas.cassandra.message.{ExPrepared, Execute}
import net.vangas.cassandra.config.QueryConfig

trait Statement

class PreparedStatement(private[cassandra] val queryAwarePrepared: ExPrepared) extends Statement {
  def bind(params: Any*): BoundStatement = new BoundStatement(this, params)
}

class BoundStatement(preparedStatement: PreparedStatement, val params: Seq[_]) extends Statement {

  def originalQuery: String = preparedStatement.queryAwarePrepared.originalQuery

  def executeRequest(implicit queryConfig: QueryConfig): Execute = {
    val queryId = preparedStatement.queryAwarePrepared.prepared.id
    Execute(queryId, queryConfig.toQueryParameters(params))
  }

}