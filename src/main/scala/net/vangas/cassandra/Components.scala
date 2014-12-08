package net.vangas.cassandra

import akka.actor.{ActorSystem, ActorRef, Actor}
import net.vangas.cassandra.connection.{DefaultStreamContext, Connection, ResponseHandler}
import java.net.InetSocketAddress
import akka.io.{IO, Tcp}

import scala.concurrent.duration.FiniteDuration

trait ConnectionComponents {
  def createResponseHandlerActor: Actor = new ResponseHandler with ResponseHandlerComponents
  def ioManager(implicit system: ActorSystem): ActorRef = IO(Tcp)
}

trait ResponseHandlerComponents {
  def createAuthentication: Actor = new Authentication
}

trait ConnectionRouterComponents {
  def createConnection(queryTimeOut: FiniteDuration,
                       connectionTimeout: FiniteDuration,
                       node: InetSocketAddress): Actor = {
    val streamContext = new DefaultStreamContext(queryTimeOut)
    new Connection(connectionTimeout, node, streamContext, ResponseFrame) with ConnectionComponents
  }
}