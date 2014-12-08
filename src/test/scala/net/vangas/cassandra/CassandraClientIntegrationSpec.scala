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

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, OneInstancePerTest, FunSpec}
import org.scalatest.Matchers._
import org.slf4j.LoggerFactory
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.joda.time.DateTime
import java.net.InetAddress

//FIXME: This test is integration test which depends on running cassandra node locally.
class CassandraClientIntegrationSpec extends FunSpec with BeforeAndAfter with BeforeAndAfterAll with OneInstancePerTest {

  val LOG = LoggerFactory.getLogger(getClass)

  val keyspace = "test_ks"

  val client = new CassandraClient(Seq("localhost"))
  val session = client.createSession(keyspace)

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
  val TABLE = s"create table table1 (id int primary key, ${columnDefs.mkString(", ")})"
  val INSERT =
    "insert into table1(id, col1, col2, col3, col4, col5, col6, col7, col8, col9) " +
      "values(111, 123, 'abc', ['a'], [22], {33,44}, {123: 'map_value'}, '2014-10-13 23:47', " +
      "a5c3921d-294a-41bc-8aff-6be0b5d17213, '192.168.3.3')"


  before {
    LOG.info("TABLE STATEMENT: {}", TABLE)

    val insertFuture = for {
      r0 <- session.execute(DROP)
      r1 <- session.execute(TABLE)
      r2 <- session.execute(INSERT)
    } yield r2
    Await.result(insertFuture, 1 second)
  }

  override def afterAll() = {
    client.close()
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
