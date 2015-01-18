package net.vangas.cassandra

import akka.actor.{ActorSystem, ActorRef, Actor}
import net.vangas.cassandra.config.Configuration
import net.vangas.cassandra.connection._
import java.net.InetSocketAddress
import akka.io.{IO, Tcp}

import scala.concurrent.duration.FiniteDuration

trait ConnectionComponents {
  def createResponseHandlerActor(node: InetSocketAddress): Actor = new ResponseHandler(node)
  def ioManager(implicit system: ActorSystem): ActorRef = IO(Tcp)
}


trait CPManagerComponents {
  def createConnection(queryTimeOut: FiniteDuration,
                       connectionTimeout: FiniteDuration,
                       node: InetSocketAddress): Actor = {
    val streamContext = new DefaultStreamContext(queryTimeOut)
    new Connection(connectionTimeout, node, streamContext, ResponseFrame) with ConnectionComponents
  }
}

trait SessionComponents {

  def createConnectionManager(sessionId: Int,
                              keyspace: String,
                              nodes: Seq[InetSocketAddress],
                              config: Configuration): Actor = {
    new ConnectionPoolManager(sessionId, keyspace, nodes, config) with CPManagerComponents
  }

  def createRequestLifecycle(loadBalancer: ActorRef,
                             connectionPoolManager: ActorRef,
                             config: Configuration): Actor = {
    new RequestLifecycle(loadBalancer, connectionPoolManager, config)
  }
}