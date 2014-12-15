package net.vangas.cassandra.example

import akka.actor.Actor
import net.vangas.cassandra.example.action._
import net.vangas.cassandra.example.model._
import org.json4s.ext.JodaTimeSerializers
import org.json4s.{DefaultFormats, Formats}
import concurrent.ExecutionContext.Implicits.global
import spray.httpx.Json4sJacksonSupport
import spray.routing.HttpService
import spray.http.StatusCodes._

class VangasTwitterExampleServiceActor(val userAction: UserAction,
                                       val tweetAction: TweetAction)
  extends Actor with VangasTwitterExampleService {

  def actorRefFactory = context

  def receive = runRoute(vangasTwitterRoute)
}


trait VangasTwitterExampleService extends HttpService with Json4sJacksonSupport {

  val userAction: UserAction
  val tweetAction: TweetAction

  implicit def json4sJacksonFormats: Formats = DefaultFormats ++ JodaTimeSerializers.all

  def vangasTwitterRoute = {
    pathPrefix("v1") {
      path("users") {
        post {
          entity(as[UserEntity]) { user =>
            onSuccess(userAction.save(user)) {
              _ => complete(Created)
            }
          }
        }
      } ~
      path("tweets" / Segment) { id =>
        onSuccess(tweetAction.get(id))(complete(_))
      } ~
      path("tweets") {
        get {
          onSuccess(tweetAction.list())(complete(_))
        } ~
        post {
          entity(as[TweetEntity]) { tweet =>
            onSuccess(tweetAction.save(tweet)) {
              _ => complete(Created)
            }
          }
        }
      } ~
      path("follow") {
        post {
          entity(as[FollowEntity]) { follow =>
            onSuccess(userAction.follow(follow.userName, follow.followedUserName)) {
              _ => complete(OK)
            }
          }
        }
      }
    }
  }
}