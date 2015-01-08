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

import java.net.InetAddress

import net.vangas.cassandra.connection.Session
import org.joda.time.DateTime
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec}
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class CassandraClientIntegrationSpec extends FunSpec with CCMSupport with BeforeAndAfterAll with BeforeAndAfter {

  val cluster = "test_cluster1"
  val keyspace = "test_ks1"

  var client: CassandraClient = _
  var session: Session = _

  val columnDefs = Seq(
    "col1 int",
    "col2 text",
    "col3 list<text>",
    "col4 list<int>",
    "col5 set<int>",
    "col6 map<int, text>",
    "col7 timestamp",
    "col8 uuid",
    "col9 inet"
  )

  val DROP = "drop table if exists table1"

  val CREATE_TABLE = CREATE_SIMPLE_TABLE.format("table1", columnDefs.mkString(", "))

  val INSERT =
    "insert into table1(id, col1, col2, col3, col4, col5, col6, col7, col8, col9) " +
      "values(111, 123, 'abc', ['a'], [22], {33,44}, {123: 'map_value'}, '2014-10-13 23:47', " +
      "a5c3921d-294a-41bc-8aff-6be0b5d17213, '192.168.3.3')"


  before {
    LOG.info("TABLE STATEMENT: {}", CREATE_TABLE)

    val insertFuture = for {
      r0 <- session.execute(DROP)
      r1 <- session.execute(CREATE_TABLE)
      r2 <- session.execute(INSERT)
    } yield r2
    Await.result(insertFuture, 1 second)
  }

  override def beforeAll() = {
    createCluster
    populate(1)
    startCluster
    createKS(1)
    client = new CassandraClient(Seq("localhost"))
    session = client.createSession(keyspace)
  }

  override def afterAll() = {
    client.close()
    stopCluster
  }

  describe("CassandraClient") {
    it("should insert/select data") {
      val select = "select * from table1"
      val result = Await.result(session.execute(select), 1 second)
      val uuid = java.util.UUID.fromString("a5c3921d-294a-41bc-8aff-6be0b5d17213")

      result.size should be (1)
      result.head.get[Int](0) should be (Some(111))
      result.head.get[Int](1) should be (Some(123))
      result.head.get[String](2) should be (Some("abc"))
      result.head.get[Seq[String]](3) should be (Some(Seq("a")))
      result.head.get[Seq[Int]](4) should be (Some(Seq(22)))
      result.head.get[Set[Int]](5) should be (Some(Set(33, 44)))
      result.head.get[Map[Int, String]](6) should be (Some(Map(123 -> "map_value")))
      result.head.get[DateTime](7) should be (Some(new DateTime(2014, 10, 13, 23, 47)))
      result.head.get[JUUID](8) should be (Some(uuid))
      result.head.get[InetAddress](9) should be (Some(InetAddress.getByName("192.168.3.3")))
    }

    it("should filter by id") {
      val select = "select * from table1 where id = ?"

      val result = Await.result(session.execute(select, 111), 1 second)
      result.size should be (1)
      result.head.get[Int](0) should be (Some(111))
    }

    it("should filter by int") {
      val createIndex = "create index idx_col1_table1 ON table1 (col1)"
      val select = "select * from table1 where col1 = ?"

      Await.result(session.execute(createIndex), 1 second)
      Thread.sleep(1000) //Wait for index creation
      val result = Await.result(session.execute(select, 123), 1 second)
      result.size should be (1)
      result.head.get[Int](0) should be (Some(111))
    }

    it("should filter by text") {
      val createIndex = "create index idx_col2_table1 ON table1 (col2)"
      val select = "select * from table1 where col2 = ?"

      Await.result(session.execute(createIndex), 1 second)
      Thread.sleep(1000) //Wait for index creation
      val result = Await.result(session.execute(select, "abc"), 1 second)
      result.size should be (1)
      result.head.get[Int](0) should be (Some(111))
    }

    it("should update list") {
      val update = "update table1 set col3 = ? where id = ?"
      val select = "select * from table1"

      Await.result(session.execute(update, Seq("b"), 111), 1 second)
      Thread.sleep(1000) //Wait for index creation
      val result = Await.result(session.execute(select), 1 second)
      result.size should be (1)
      result.head.get[Int](0) should be (Some(111))
      result.head.get[Seq[String]](3) should be (Some(Seq("b")))
    }

    it("should update set") {
      val update = "update table1 set col5 = ? where id = ?"
      val select = "select * from table1"

      Await.result(session.execute(update, Set(10,3,44), 111), 1 second)
      Thread.sleep(1000) //Wait for index creation
      val result = Await.result(session.execute(select), 1 second)
      result.size should be (1)
      result.head.get[Int](0) should be (Some(111))
      result.head.get[Set[Int]](5) should be (Some(Set(10,3,44)))
    }

    it("should filter by uuid") {
      val uuid = java.util.UUID.fromString("a5c3921d-294a-41bc-8aff-6be0b5d17213")
      val createIndex = "create index idx_col8_table1 ON table1 (col8)"
      val correctUuidSelect = "select * from table1 where col8 = ?"

      Await.result(session.execute(createIndex), 1 second)
      Thread.sleep(1000) //Wait for index creation
      val result = Await.result(session.execute(correctUuidSelect, uuid), 1 second)
      result.size should be (1)
      result.head.get[Int](0) should be (Some(111))
    }

  }
}
