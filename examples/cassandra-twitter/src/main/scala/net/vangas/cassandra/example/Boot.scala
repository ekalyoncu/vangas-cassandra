package net.vangas.cassandra.example

import scala.concurrent.ExecutionContext
import net.vangas.cassandra.example.action._
import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http


object Boot extends App {

  implicit val system = ActorSystem("vangas-cassandra-twitter")
  implicit val executor = ExecutionContext.Implicits.global

  val userAction = new UserAction
  val tweetAction = new TweetAction

  val service = system.actorOf(Props(new VangasTwitterExampleServiceActor(userAction, tweetAction)), "vangas-twitter-service")

  IO(Http) ! Http.Bind(service, "localhost", port = 8080)
}
