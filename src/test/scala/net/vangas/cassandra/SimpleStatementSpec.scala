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

package net.vangas.cassandra

import net.vangas.cassandra.config.QueryConfig
import net.vangas.cassandra.message.Query
import org.scalatest.FunSpec
import org.scalatest.Matchers._

class SimpleStatementSpec extends FunSpec {

  describe("SimpleStatement") {
    it("should be created from factory") {
      Statement("query") should be(new SimpleStatement("query"))
      Statement("query", "param1", 1, Seq("a1", "a2")) should be(new SimpleStatement("query", Seq("param1", 1, Seq("a1", "a2"))))
    }

    it("should create request message") {
      implicit val queryConfig = QueryConfig()

      val request1 = Statement("query").toRequestMessage
      request1 should be(Query("query", QueryParameters()))

      val request2 = Statement("query_with_param", "param1", 1).toRequestMessage
      request2 should be(Query("query_with_param", QueryParameters(values = Seq("param1", 1))))
    }

  }

}
