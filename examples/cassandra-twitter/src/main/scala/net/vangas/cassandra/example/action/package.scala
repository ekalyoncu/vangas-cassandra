package net.vangas.cassandra.example

import java.util.UUID

import net.vangas.cassandra.pimped._
import net.vangas.cassandra.util.DateUtils.toDateTime
import net.vangas.cassandra.CassandraClient
import net.vangas.cassandra.example.model.{UserEntity, TweetEntity}
import scala.concurrent.{ExecutionContext, Future}

package object action {

  val cassandraClient = new CassandraClient(Seq("localhost")) // per cluster
  val session = cassandraClient.createSession("vangas_twitter") // per keyspace

  class UserAction(implicit executor : ExecutionContext) {
    val INSERT_PREPARED = session.prepare("INSERT INTO users(username, password) VALUES(?,?)")
    def save(user: UserEntity): Future[_] = {
      INSERT_PREPARED.flatMap { statement =>
        session.execute(statement.bind(user.userName, user.password))
      }
    }
  }

  class TweetAction(implicit executor : ExecutionContext) {
    val INSERT_PREPARED = session.prepare("INSERT INTO tweets(tweet_id, username, body) VALUES(now(), ?,?)")

    def save(tweet: TweetEntity): Future[_] = {
      INSERT_PREPARED.flatMap { statement =>
        session.execute(statement.bind(tweet.userName, tweet.body))
      }
    }

    def list(): Future[Iterable[TweetEntity]] = {
      session.execute("SELECT tweet_id, username, body, dateOf(tweet_id) FROM tweets").mapRows { row =>
        TweetEntity(
          row.get[UUID](0).map(_.toString),
          row.getOrNull[String](1),
          row.getOrNull[String](2),
          row.get[UUID](0).map(toDateTime))
      }
    }

    def get(tweetId: String): Future[Option[TweetEntity]] = {
      session.execute("SELECT * FROM tweets WHERE tweet_id = ?", UUID.fromString(tweetId)).mapRow { row =>
        TweetEntity(
          row.get[UUID](0).map(_.toString),
          row.getOrNull[String](1),
          row.getOrNull[String](2),
          row.get[UUID](0).map(toDateTime))
      }
    }
  }
}
