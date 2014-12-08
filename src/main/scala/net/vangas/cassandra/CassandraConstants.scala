package net.vangas.cassandra

import java.nio.charset.Charset

object CassandraConstants {
  val VERSION_FOR_V1: Byte = 0x01
  val VERSION_FOR_V2: Byte = 0x02
  val VERSION_FOR_V3: Byte = 0x03
  val FLAGS: Byte = 0x00

  val UTF_8 = Charset.forName("UTF-8")

  val US_ASCII = Charset.forName("US-ASCII")

  //OP_CODES
  val ERROR: Byte = 0x00
  val STARTUP: Byte = 0x01
  val READY: Byte = 0x02
  val AUTHENTICATE: Byte = 0x03
  val OPTIONS: Byte = 0x05
  val SUPPORTED: Byte = 0x06
  val QUERY: Byte = 0x07
  val RESULT: Byte = 0x08
  val PREPARE: Byte = 0x09
  val EXECUTE: Byte = 0x0A
  val REGISTER: Byte = 0x0B
  val EVENT: Byte = 0x0C
  val BATCH: Byte = 0x0D
  val AUTH_CHALLENGE: Byte = 0x0E
  val AUTH_RESPONSE: Byte = 0x0F
  val AUTH_SUCCESS: Byte = 0x10

  //ERROR_CODES
  val SERVER_ERROR = 0x0000
  val PROTOCOL_ERROR = 0x000A
  val BAD_CREDENTIALS = 0x0100
  val UNAVAILABLE_EXCEPTION = 0x1000
  val OVERLOADED = 0x1001
  val BOOTSTRAPPING = 0x1002
  val TRUNCATE_ERROR = 0x1003
  val WRITE_TIMEOUT = 0x1100
  val READ_TIMEOUT = 0x1200
  val SYNTAX_ERROR = 0x2000
  val UNAUTHORIZED = 0x2100
  val INVALID_QUERY = 0x2200
  val CONFIG_ERROR = 0x2300
  val ALREADY_EXIST = 0x2400
  val UNPREPARED = 0x2500

  //CONSISTENCY
  val ANY: Short = 0x0000
  val ONE: Short = 0x0001
  val TWO: Short = 0x0002
  val THREE: Short = 0x0003
  val QUORUM: Short = 0x0004
  val ALL: Short = 0x0005
  val LOCAL_QUORUM: Short = 0x0006
  val EACH_QUORUM: Short = 0x0007
  val SERIAL: Short = 0x0008
  val LOCAL_SERIAL: Short = 0x0009
  val LOCAL_ONE: Short = 0x000A

  //RESULT KINDS
  val VOID = 0x0001
  val ROWS = 0x0002
  val SET_KEYSPACE = 0x0003
  val PREPARED = 0x0004
  val SCHEMA_CHANGE = 0x0005

  //ROW FLAGS
  val GLOBAL_TABLE_SPEC = 0x0001
  val HAS_MORE_PAGES = 0x0002
  val NO_METADATA = 0x0004


  //OPTION IDS - DATA_TYPES
  val TYPE_CUSTOM = 0x0000
  val TYPE_ASCII = 0x0001
  val TYPE_BIGINT =  0x0002
  val TYPE_BLOB = 0x0003
  val TYPE_BOOLEAN = 0x0004
  val TYPE_COUNTER = 0x0005
  val TYPE_DECIMAL = 0x0006
  val TYPE_DOUBLE = 0x0007
  val TYPE_FLOAT = 0x0008
  val TYPE_INT = 0x0009
  val TYPE_TIME_STAMP = 0x000B
  val TYPE_UUID = 0x000C
  val TYPE_VARCHAR = 0x000D
  val TYPE_VARINT =  0x000E
  val TYPE_TIME_UUID = 0x000F
  val TYPE_INET = 0x0010
  val TYPE_LIST = 0x0020
  val TYPE_MAP = 0x0021
  val TYPE_SET = 0x0022
  val TYPE_UDT = 0x0030
  val TYPE_TUPLE = 0x0031

}

