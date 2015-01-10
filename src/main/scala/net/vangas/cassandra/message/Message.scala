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

package net.vangas.cassandra.message

import akka.util.{ByteIterator, ByteStringBuilder, ByteString}
import net.vangas.cassandra._
import net.vangas.cassandra.CassandraConstants._
import net.vangas.cassandra.util.ByteUtils._
import java.net.{InetAddress, InetSocketAddress}

sealed trait Message

sealed trait RequestMessage extends Message {
  def serialize: ByteString
}

sealed trait ResponseMessage extends Message


/**
 * Initialize the connection. The server will respond by either a READY message
 * (in which case the connection is ready for queries) or an AUTHENTICATE message
 * (in which case credentials will need to be provided using AUTH_RESPONSE).
 */
case object Startup extends RequestMessage {
  private[this] val CQL_VERSION_OPTION = "CQL_VERSION"
  private[this] val CQL_VERSION = "3.0.0"
  private[this] val STARTUP_BODY_MAP_LEN = 1

  override def serialize: ByteString = {
    new ByteStringBuilder()
      .putShort(STARTUP_BODY_MAP_LEN)
      .putShort(CQL_VERSION_OPTION.length)
      .append(ByteString.fromString(CQL_VERSION_OPTION))
      .putShort(CQL_VERSION.length)
      .append(ByteString.fromString(CQL_VERSION))
      .result()
  }
}

/**
 * Answers a server authentication challenge.
 *
 * Authentication in the protocol is SASL based. The server sends authentication
 * challenges (a bytes token) to which the client answer with this message. Those
 * exchanges continue until the server accepts the authentication by sending a
 * AUTH_SUCCESS message after a client AUTH_RESPONSE. It is however that client that
 * initiate the exchange by sending an initial AUTH_RESPONSE in response to a
 * server AUTHENTICATE request.
 *
 */
case object AuthResponse extends RequestMessage {
  def serialize: ByteString = ???
}

/**
 * Asks the server to return what STARTUP options are supported
 */
case object Options extends RequestMessage {
  def serialize: ByteString = ???
}

/**
 *  Performs a CQL query
 */
case class Query(query: String, params: QueryParameters) extends RequestMessage {
  def serialize: ByteString = {
    val queryBytes = query.getBytes
    new ByteStringBuilder()
      .putInt(queryBytes.length)
      .putBytes(queryBytes)
      .append(params.serialize)
      .result()
  }
}

/**
 * Prepare a query for later execution (through EXECUTE). The body consists of
 * the CQL query to prepare as a [long string].
 * The server will respond with a RESULT message with a `prepared` kind
 */
case class Prepare(query: String) extends RequestMessage {
  def serialize: ByteString = {
    val queryBytes = query.getBytes
    new ByteStringBuilder().putInt(queryBytes.length).putBytes(queryBytes).result()
  }
}

/**
 * Executes a prepared query. The body of the message must be:
 * <id><query_parameters>
 * where <id> is the prepared query ID. It's the [short bytes] returned as a
 * response to a PREPARE message
 */
case class Execute(queryId: PreparedId, queryParameters: QueryParameters) extends RequestMessage {
  def serialize: ByteString = {
    val builder = new ByteStringBuilder()
    builder.putShort(queryId.id.length)
    builder.putBytes(queryId.id)
    builder.append(queryParameters.serialize)
    builder.result()
  }
}

/**
 * TODO:
 */
case object Batch extends RequestMessage {
  def serialize: ByteString = ???
}


/**
 * Register this connection to receive some type of events. The body of the
 * message is a [string list] representing the event types to register to.
 *
 * The response to a REGISTER message will be a READY message.
 */
//  case object Register extends RequestMessage


/**
 * Indicates an error processing a request. The body of the message will be an
 * error code ([int]) followed by a [string] error message. Then, depending on
 * the exception, more content may follow.
 */
case class Error(errorCode: Int, message: String) extends ResponseMessage {
  override def toString: String = s"ErrorCode: $errorCode. $message"
}

case class NodeAwareError(error: Error, node: InetSocketAddress) {
  override def toString: String = s"$error. Node[$node]"
}

object Error {

  def apply(data: ByteString): Error = {
    val byteIter = data.iterator
    val errorCode = byteIter.getInt
    Error(errorCode, readString(byteIter))
  }
}

/**
 * Indicates that the server is ready to process queries. This message will be
 * sent by the server either after a STARTUP message if no authentication is
 * required, or after a successful CREDENTIALS message.
 *
 * The body of a READY message is empty.
 */
case object Ready extends ResponseMessage


/**
 * Indicates that the server require authentication, and which authentication
 * mechanism to use.
 */
case class Authenticate(token: ByteString) extends ResponseMessage

/**
 * Indicates which startup options are supported by the server. This message
 * comes as a response to an OPTIONS message.
 *
 * The body of a SUPPORTED message is a [string multimap]. This multimap gives
 * for each of the supported STARTUP options, the list of supported values.
 */
case object Supported extends ResponseMessage



sealed trait Kind

case object Void extends Kind

case class SetKeyspace(name: String) extends Kind

case class SchemaChange(changeType: String, target: String, options: String) extends Kind

case class Rows(metaData: MetaData,
                count: Int,
                content: IndexedSeq[Row]) extends Kind

class PreparedId(val id: Array[Byte]) {
  override def hashCode(): Int = java.util.Arrays.hashCode(id)

  override def equals(obj: Any): Boolean = obj match {
    case that: PreparedId => java.util.Arrays.equals(id, that.id)
    case _ => false
  }
}

case class ExPrepared(prepared: Prepared, originalQuery: String, node: InetSocketAddress)
case class Prepared(id: PreparedId, metaData: MetaData, resultMetaData: MetaData) extends Kind

object Prepared {
  def apply(body: ByteIterator, metaDataFactory: Factory[ByteIterator, MetaData] = MetaData): Prepared = {
    Prepared(new PreparedId(readShortBytes(body).get), metaDataFactory(body), metaDataFactory(body))
  }
}

object Rows {

  def apply(body: ByteIterator, metaDataFactory: Factory[ByteIterator, MetaData] = MetaData): Rows = {
    val metaData = metaDataFactory(body)
    val rowsCount = body.getInt

    val content: IndexedSeq[Row] =
    (0 until rowsCount).map { _ =>
      val rowContent =
        (0 until metaData.columnsCount).map { _ =>
          readByteString(body)
        }
      new RowImpl(metaData, rowContent)
    }

    Rows(metaData, rowsCount, content)
  }

}


/**
 * The result to a query (QUERY, PREPARE, EXECUTE or BATCH messages).
 * Kinds:
 *  - Void
 *  - Rows
 *  - Set_keyspace
 *  - Prepared
 *  - Schema_change
 */
case class Result(body: Kind) extends ResponseMessage

object Result {

  def apply(data: ByteString): Result = {
    val byteIter = data.iterator
    byteIter.getInt match {
      case VOID =>
        Result(Void)
      case ROWS =>
        Result(Rows(byteIter))
      case SET_KEYSPACE =>
        Result(SetKeyspace(readString(byteIter)))
      case SCHEMA_CHANGE => Result(SchemaChange(null, null, null))
      case PREPARED => Result(Prepared(byteIter))
    }
  }
}

/**
 * And event pushed by the server. A client will only receive events for the
 * type it has REGISTER to.
 * All EVENT message have a streamId of -1.
 */
trait Event extends ResponseMessage

object Event {
  def apply(data: ByteString): Event = {
    val byteIter = data.iterator
    val eventType = readString(byteIter)
    val changeType = readString(byteIter)
    eventType match {
      case "TOPOLOGY_CHANGE" =>
        val node = INET.deserialize(byteIter.toByteString)
        TopologyChangeEvent(TopologyChangeType.withName(changeType), node)
      case "STATUS_CHANGE" =>
        val node = INET.deserialize(byteIter.toByteString)
        StatusChangeEvent(StatusChangeType.withName(changeType), node)
      case "SCHEMA_CHANGE" => SchemaChangeEvent
      case x => throw new IllegalArgumentException(s"Unknown event type: $x")
    }
  }
}

object TopologyChangeType extends Enumeration { val NEW_NODE, REMOVED_NODE = Value }
object StatusChangeType extends Enumeration { val UP, DOWN = Value }
object SchemaChangeType extends Enumeration { val CREATED, UPDATED, DROPPED = Value }

case class TopologyChangeEvent(changeType: TopologyChangeType.Value, node: InetAddress) extends Event
case class StatusChangeEvent(changeType: StatusChangeType.Value, node: InetAddress) extends Event
case object SchemaChangeEvent extends Event //Not implemented yet

/**
 * A server authentication challenge.
 *
 * Clients are expected to answer the server challenge by an AUTH_RESPONSE
 * message.
 */
case object AuthChallenge extends ResponseMessage

/**
 * Indicate the success of the authentication phase.
 */
case object AuthSuccess extends ResponseMessage


