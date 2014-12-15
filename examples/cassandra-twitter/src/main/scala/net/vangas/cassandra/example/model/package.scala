package net.vangas.cassandra.example

import org.joda.time.DateTime

package object model {

  case class TweetEntity(id: Option[String], userName: String, body: String, creationTime: Option[DateTime])
  case class UserEntity(userName: String, password: String)

}
