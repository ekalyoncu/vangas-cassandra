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
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import net.vangas.cassandra.message.{ExPrepared, Execute, PreparedId, Prepared}
import net.vangas.cassandra.config.QueryConfig

class BoundStatementSpec extends FunSpec {

  describe("BoundStatement") {

    it("should create execute request") {
      val prepared = new Prepared(new PreparedId("ID".getBytes), null, null)
      val preparedStatement = new PreparedStatement(ExPrepared(prepared, "QUERY", null))
      val boundStatement = new BoundStatement(preparedStatement, Seq(1,2))
      val queryParameters = new QueryParameters(ConsistencyLevel.ONE, Seq(1,2), false, -1, None, ConsistencyLevel.SERIAL)
      val e1 = boundStatement.executeRequest(QueryConfig())
      val e2 = Execute(new PreparedId("ID".getBytes), queryParameters)
      e1.queryId should be(e2.queryId)
      e1.queryParameters.serialize should be(e2.queryParameters.serialize)
    }
  }

}
