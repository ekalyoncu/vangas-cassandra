package net.vangas.cassandra.message


import org.scalatest.FunSpec
import org.scalatest.Matchers._
import akka.util.ByteStringBuilder
import net.vangas.cassandra.byteOrder
import net.vangas.cassandra.{ConsistencyLevel, QueryParameters}

class ExecuteSpec extends FunSpec {

  describe("Execute") {
    it("should serialize") {
      val queryParameters = new QueryParameters(ConsistencyLevel.QUORUM, Seq.empty, false, -1, None, ConsistencyLevel.SERIAL)
      val execute = Execute(new PreparedId("ID".getBytes), queryParameters)
      val bytes = new ByteStringBuilder()
        .putShort("ID".getBytes.length).putBytes("ID".getBytes)
        .append(queryParameters.serialize)
        .result()
      execute.serialize should be(bytes)
    }
  }
}
