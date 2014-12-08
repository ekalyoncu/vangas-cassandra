package net.vangas.cassandra

import org.apache.commons.exec.{CommandLine, DefaultExecutor}
import org.scalatest.{Suite, BeforeAndAfterAll}
import org.slf4j.LoggerFactory

trait CCMSupport extends BeforeAndAfterAll { this: Suite =>

  val LOG = LoggerFactory.getLogger(getClass)

  val CREATE_KEY_SPACE = "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }"
  val CREATE_SIMPLE_TABLE = "CREATE TABLE %s (id int PRIMARY KEY, %s)"

  val QUOTE = "\""

  val CASSANDRA_VERSION = "2.1.0"

  val keyspace: String

  val executor = new DefaultExecutor()

  val ccmPath = {
    val fullCcmPath = System.getProperty("ccm.path")
    if (fullCcmPath != null) fullCcmPath else "ccm"
  }

  override protected def beforeAll() {
  }

  override protected def afterAll() {
  }

  def createCluster(name: String) {
    executor.execute(CommandLine.parse(s"$ccmPath create $name -v $CASSANDRA_VERSION"))
  }

  def populate(nodes: Int) {
    executor.execute(CommandLine.parse(s"$ccmPath populate -n $nodes"))
  }

  def startCluster {
    executor.execute(CommandLine.parse(s"$ccmPath start"))
  }

  def createTableStatement(tableName: String, columnDefs: Seq[String]) =
    CREATE_SIMPLE_TABLE.format(tableName, columnDefs.mkString(", "))


}
