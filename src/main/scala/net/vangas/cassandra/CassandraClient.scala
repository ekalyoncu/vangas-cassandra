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

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.connection.{DefaultSession, Session}
import org.slf4j.LoggerFactory

class CassandraClient(nodeAddresses: Seq[String], port: Int = 9042) {
  import net.vangas.cassandra.CassandraClient._

  val id = clientId.incrementAndGet()
  LOG.info(s"Creating CassandraClient-$id...")
  private implicit val system = ActorSystem(s"CassandraClient-$id")

  /**
   * Creates a new session
   * @param keyspace keyspace to be queried
   * @param config Configuration to be used for connection and queries to cassandra nodes
   * @return Session instance
   */
  def createSession(keyspace: String, config: Configuration = Configuration()): Session = {
    val nodes = nodeAddresses.map(new InetSocketAddress(_, port))
    DefaultSession(keyspace, nodes, config)
  }

  /**
   * Closes this client.
   *
   * @param waitToClose True if client needs to block until client closes completely.
   */
  def close(waitToClose: Boolean = false): Unit = {
    system.shutdown()
    if (waitToClose) {
      system.awaitTermination()
    }
  }
}

private object CassandraClient {
  val LOG = LoggerFactory.getLogger("CassandraClient")
  val clientId = new AtomicInteger(0)
}