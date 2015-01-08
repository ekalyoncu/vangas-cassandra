package net.vangas.cassandra

import scala.sys.process._
import org.scalatest.{Suite, BeforeAndAfterAll}
import org.slf4j.LoggerFactory

trait CCMSupport extends BeforeAndAfterAll { this: Suite =>

  val LOG = LoggerFactory.getLogger(getClass)

  val cluster: String
  val keyspace: String

  lazy val CREATE_KEY_SPACE = s"CREATE KEYSPACE $keyspace WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d };"
  lazy val CREATE_SIMPLE_TABLE = "CREATE TABLE %s (id int PRIMARY KEY, %s);"

  val CASSANDRA_VERSION = "2.1.0"

  val ccmPath = {
    val fullCcmPath = System.getProperty("ccm.path")
    if (fullCcmPath != null) fullCcmPath else "ccm"
  }

  def createCluster = {
    LOG.info(s"Creating cluster $cluster...")
    Seq(ccmPath, "create", s"$cluster", "-v", s"$CASSANDRA_VERSION").!
  }

  def populate(nodes: Int) = {
    LOG.info(s"Populating $cluster with $nodes nodes...")
    Seq(ccmPath, "populate", "-n", s"$nodes").!
  }

  def startCluster = {
    LOG.info(s"Starting cluster $cluster...")
    s"$ccmPath start".!
  }

  def stopCluster = {
    LOG.info(s"Stopping cluster $cluster...")
    s"$ccmPath stop".!
    s"$ccmPath remove".!
  }

  def createKS(replication: Int) = {
    LOG.info(s"Creating keyspace[$keyspace] in cluster $cluster...")
    (Seq("echo", CREATE_KEY_SPACE.format(replication)) #| Seq(ccmPath, "node1", "cqlsh")).!
  }

}
