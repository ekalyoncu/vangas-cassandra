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

package net.vangas.cassandra.integration

import net.vangas.cassandra.{ResultSet, CassandraClient, CCMSupport}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import net.vangas.cassandra.VangasTestHelpers._

class RoundRobinLoadBalancingIntegrationSpec extends FunSpec with CCMSupport with BeforeAndAfterAll with BeforeAndAfter {

  val cluster = "rrlb_cluster"
  val keyspace = "rrlb_ks1"

  lazy val client = new CassandraClient(Seq("127.0.0.1", "127.0.0.2"))
  lazy val session = client.createSession(keyspace)

  override def afterAll() = {
    client.close()
  }

  describe("RoundRobinLoadBalancingPolicy") {
    it("should second host when first is down in one dc") {
      setupCluster(2, 2) {
        val insertFuture = for {
          r0 <- session.execute("DROP TABLE IF EXISTS table1")
          r1 <- session.execute("CREATE TABLE table1 (id int PRIMARY KEY, name text)")
          r2 <- session.execute("INSERT INTO table1(id, name) VALUES(1, 'test1')")
        } yield r2
        Await.result(insertFuture, 1 second)

        query().executionInfo().triedNodes should be(Seq(node("127.0.0.2")))

        stopNode(1)

        query().executionInfo().triedNodes should be(Seq(node("127.0.0.2")))
      }
    }

    //FIXME: Not implemented yet
    it("should add new node and update loadbalancer") {
      setupCluster(1, 2) {
        val insertFuture = for {
          r0 <- session.execute("DROP TABLE IF EXISTS table1")
          r1 <- session.execute("CREATE TABLE table1 (id int PRIMARY KEY, name text)")
          r2 <- session.execute("INSERT INTO table1(id, name) VALUES(1, 'test1')")
        } yield r2
        Await.result(insertFuture, 1 second)

        query().executionInfo().triedNodes should be(Seq(node("127.0.0.1")))

        stopNode(1)

        addNode(2, "127.0.0.2")

        Thread.sleep(1200) // Wait for node up event to be fired

        query().executionInfo().triedNodes should be(Seq(node("127.0.0.2")))
      }
    }
  }

  private def query(): ResultSet = Await.result(session.execute("SELECT * FROM table1"), 1 second)
}


