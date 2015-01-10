package net.vangas.cassandra.connection

import org.joda.time.{DateTimeUtils, DateTime}
import org.scalatest.{OneInstancePerTest, BeforeAndAfter, FunSpec}
import org.scalatest.Matchers._
import net.vangas.cassandra.RequestStream
import net.vangas.cassandra.message.Query
import scala.collection.mutable
import scala.concurrent.duration._

class DefaultStreamContextSpec extends FunSpec with BeforeAndAfter with OneInstancePerTest {

  private val ttl = 10 seconds
  val context = new DefaultStreamContext(ttl)

  after {
    DateTimeUtils.setCurrentMillisSystem()
  }

  describe("DefaultStreamContext") {
    it("should registerStream") {
      val streamId = context.registerStream(RequestStream(null, Query("q", null)))
      streamId should be(Some(0))
      context.streams.size should be(1)
      context.streams(0).requester should be(null)
      context.streams(0).originalRequest should be(Query("q", null))
    }

    it("should release streamid") {
      val streamId1 = context.registerStream(RequestStream(null, Query("q", null)))
      val streamId2 = context.registerStream(RequestStream(null, Query("q", null)))
      streamId1 should be(Some(0))
      streamId2 should be(Some(1))
      context.streams.size should be(2)
      context.releaseStream(streamId1.get)
      context.streams.size should be(1)
      val streamId3 = context.registerStream(RequestStream(null, Query("q", null)))
      streamId3 should be(Some(0))
      context.streams.size should be(2)
    }

    it("should reach max simultaneous stream limit") {
      for(i <- 0 until 128) {
        context.streamIds(i) = true
      }
      context.registerStream(RequestStream(null, null)) should be(None)
    }

    it("should clean expired streams") {
      val firstStreamRequest = RequestStream(null, Query("q1", null))
      context.registerStream(firstStreamRequest)

      val moreThanTTL = DateTime.now().plusSeconds(ttl.plus(2 second).toSeconds.toInt).getMillis
      DateTimeUtils.setCurrentMillisFixed(moreThanTTL)
      val secondStreamRequest = RequestStream(null, Query("q2", null))
      context.registerStream(secondStreamRequest)

      context.streamIds(0) should be(true)
      context.streamIds(1) should be(true)
      context.streams should be(mutable.Map(0.toShort -> firstStreamRequest, 1.toShort -> secondStreamRequest))

      context.cleanExpiredStreams()

      context.streamIds(0) should be(false) //first stream is available now
      context.streamIds(1) should be(true) //second stream is still in use
      context.streams should be(mutable.Map(1.toShort -> secondStreamRequest))
    }
  }

}
