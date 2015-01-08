package net.vangas.cassandra

import akka.util.{ByteStringBuilder, ByteString}
import net.vangas.cassandra.ConsistencyLevel.ConsistencyLevel
import scala.collection.mutable
import net.vangas.cassandra.util.ByteUtils.valuesToByteString

private[cassandra] case class QueryParameters(consistency: ConsistencyLevel = ConsistencyLevel.ONE,
                                              values: Seq[_] = Seq.empty,
                                              skipMetaData: Boolean = false,
                                              pageSize: Int = -1,
                                              pagingState: Option[ByteString] = None,
                                              serialConsistency: ConsistencyLevel = ConsistencyLevel.SERIAL) {
  var flagsSet = mutable.Set.empty[FlagOption.Value]

  val flags = {
    if (values.nonEmpty) flagsSet += FlagOption.VALUES
    if (skipMetaData) flagsSet += FlagOption.SKIP_METADATA
    if (pageSize > 0) flagsSet += FlagOption.PAGE_SIZE
    if (pagingState.isDefined) flagsSet += FlagOption.PAGING_STATE
    if (serialConsistency != ConsistencyLevel.SERIAL) flagsSet += FlagOption.SERIAL_CONSISTENCY
    new Flags(flagsSet.toSet)
  }

  def serialize: ByteString = {
    val builder = new ByteStringBuilder()
      .putShort(consistency.id)
      .putByte(flags.serialize)
    if (flagsSet.contains(FlagOption.VALUES)) {
      builder.append(valuesToByteString(values:_*))
    }
    if (flagsSet.contains(FlagOption.PAGE_SIZE)) {
      builder.putInt(pageSize)
    }
    if (flagsSet.contains(FlagOption.PAGING_STATE)) {
      builder.append(pagingState.get)
    }
    builder.result()
  }
}

object ConsistencyLevel extends Enumeration {
  type ConsistencyLevel = Value

  val ANY = Value(0x0000)
  val ONE = Value(0x0001)
  val TWO = Value(0x0002)
  val THREE = Value(0x0003)
  val QUORUM = Value(0x0004)
  val ALL = Value(0x0005)
  val LOCAL_QUORUM = Value(0x0006)
  val EACH_QUORUM = Value(0x0007)
  val SERIAL = Value(0x0008)
  val LOCAL_SERIAL = Value(0x0009)
  val LOCAL_ONE = Value(0x000A)
}

object FlagOption extends Enumeration {
  type FlagsOption = Value
  val VALUES = Value(0x01)
  val SKIP_METADATA = Value(0x02)
  val PAGE_SIZE = Value(0x04)
  val PAGING_STATE = Value(0x08)
  val SERIAL_CONSISTENCY = Value(0x10)
  val DEFAULT_TIMESTAMP = Value(0x20)
  val VALUE_NAMES = Value(0x40)
}


class Flags(flagSet: Set[FlagOption.Value]) {
  def serialize: Byte = flagSet.foldLeft(0)(_ | _.id).toByte
}
