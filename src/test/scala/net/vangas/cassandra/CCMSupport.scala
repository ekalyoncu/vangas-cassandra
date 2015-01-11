package net.vangas.cassandra

import scala.sys.process._
import org.scalatest.{Matchers, Suite, BeforeAndAfterAll}
import org.slf4j.LoggerFactory

trait CCMSupport extends BeforeAndAfterAll with Matchers { this: Suite =>

  val LOG = LoggerFactory.getLogger(getClass)

  val cluster: String
  val keyspace: String

  lazy val CREATE_SIMPLE_KEY_SPACE =
    s"CREATE KEYSPACE $keyspace WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d };"

  lazy val CREATE_MULTI_DC_KEY_SPACE =
    s"CREATE KEYSPACE $keyspace WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : %d, 'dc2' : %d};"

  lazy val CREATE_SIMPLE_TABLE = "CREATE TABLE %s (id int PRIMARY KEY, %s)"

  val CASSANDRA_VERSION = "2.1.0"

  val ccmPath = {
    val fullCcmPath = System.getProperty("ccm.path")
    if (fullCcmPath != null) fullCcmPath else "ccm"
  }

  def createCluster(nodes: Int) = {
    LOG.info(s"Creating cluster $cluster...")
    Seq(ccmPath, "create", s"$cluster", "-n" , s"$nodes" , "-v", s"$CASSANDRA_VERSION").!
  }

  def createClusterWith2DC(nodesInDC1: Int, nodesInDC2: Int) = {
    LOG.info(s"Creating cluster $cluster...")
    Seq(ccmPath, "create", s"$cluster", "-n" , s"$nodesInDC1:$nodesInDC2" , "-v", s"$CASSANDRA_VERSION").!
  }

  def stopNode(nodeNr: Int) = {
    LOG.info(s"Stopping node$nodeNr...")
    Seq(ccmPath, s"node$nodeNr", "stop").!
  }

  def addNode(nodeNr: Int, host: String) = {
    LOG.info(s"Adding node$nodeNr")
    Seq(ccmPath, "add", s"node$nodeNr", s"-i $host").!
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
    (Seq("echo", CREATE_SIMPLE_KEY_SPACE.format(replication)) #| Seq(ccmPath, "node1", "cqlsh")).!
  }

  def createKSWith2DC(replicationInDC1: Int, replicationInDC2: Int) = {
    LOG.info(s"Creating keyspace[$keyspace] in cluster $cluster...")
    (Seq("echo", CREATE_MULTI_DC_KEY_SPACE.format(replicationInDC1, replicationInDC2)) #| Seq(ccmPath, "node1", "cqlsh")).!
  }

  def setupCluster(nodes: Int, replicationFactor: Int = 1)(runTest: => Unit) {
    try {
      createCluster(nodes)
      startCluster
      createKS(replicationFactor)
      runTest
    } finally {
      stopCluster
    }
  }

  def setupClusterWith2DC(nodesInDC1: Int,
                          nodesInDC2: Int,
                          replicationInDC1: Int,
                          replicationInDC2: Int)(runTest: => Unit) {
    try {
      createClusterWith2DC(nodesInDC1, nodesInDC2)
      startCluster
      createKSWith2DC(replicationInDC1, replicationInDC2)
      runTest
    } finally {
      stopCluster
    }
  }


}
