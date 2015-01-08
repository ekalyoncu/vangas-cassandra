/*
 * Copyright (C) 2015 Egemen Kalyoncu
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

package net.vangas.cassandra.error

object RequestErrorCode extends Enumeration {
  type QueryErrorCode = Value

  //Cassandra errors
  val SERVER_ERROR = Value
  val PROTOCOL_ERROR = Value
  val BAD_CREDENTIALS = Value
  val UNAVAILABLE_EXCEPTION = Value
  val OVERLOADED = Value
  val BOOTSTRAPPING = Value
  val TRUNCATE_ERROR = Value
  val WRITE_TIMEOUT = Value
  val READ_TIMEOUT = Value
  val SYNTAX_ERROR = Value
  val UNAUTHORIZED = Value
  val INVALID_QUERY = Value
  val CONFIG_ERROR = Value
  val ALREADY_EXIST = Value
  val UNPREPARED = Value

  //Driver errors
  val NO_HOST_AVAILABLE = Value
  val UNPREPARED_WITH_INCONSISTENT_STATEMENT = Value
  val UNKNOWN = Value
}

case class RequestError(code: RequestErrorCode.Value, msg: String)

object RequestError {
  import net.vangas.cassandra.CassandraConstants._
  import net.vangas.cassandra.message.Error

  def apply(error: Error): RequestError = {
    error match {
      case Error(SERVER_ERROR, msg) =>   RequestError(RequestErrorCode.SERVER_ERROR, msg)
      case Error(PROTOCOL_ERROR, msg) => RequestError(RequestErrorCode.PROTOCOL_ERROR, msg)
      case Error(BAD_CREDENTIALS, msg) => RequestError(RequestErrorCode.BAD_CREDENTIALS, msg)
      case Error(UNAVAILABLE_EXCEPTION, msg) => RequestError(RequestErrorCode.UNAVAILABLE_EXCEPTION, msg)
      case Error(OVERLOADED, msg) => RequestError(RequestErrorCode.OVERLOADED, msg)
      case Error(BOOTSTRAPPING, msg) => RequestError(RequestErrorCode.BOOTSTRAPPING, msg)
      case Error(TRUNCATE_ERROR, msg) => RequestError(RequestErrorCode.TRUNCATE_ERROR, msg)
      case Error(WRITE_TIMEOUT, msg) => RequestError(RequestErrorCode.WRITE_TIMEOUT, msg)
      case Error(READ_TIMEOUT, msg) => RequestError(RequestErrorCode.READ_TIMEOUT, msg)
      case Error(SYNTAX_ERROR, msg) => RequestError(RequestErrorCode.SYNTAX_ERROR, msg)
      case Error(UNAUTHORIZED, msg) => RequestError(RequestErrorCode.UNAUTHORIZED, msg)
      case Error(INVALID_QUERY, msg) => RequestError(RequestErrorCode.INVALID_QUERY, msg)
      case Error(CONFIG_ERROR, msg) => RequestError(RequestErrorCode.CONFIG_ERROR, msg)
      case _ => RequestError(RequestErrorCode.UNKNOWN, s"Unknown error: $error")
    }
  }
}


          
        
       
 
            
         
        
         
          
          
          
         
          
         
            