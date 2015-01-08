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

package net.vangas.cassandra.connection

import akka.actor.{Props, ActorSystem, ActorRef}
import akka.pattern._
import net.vangas.cassandra.config.Configuration
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Interface between session and ActorSystem.
 * This class is responsible to create actors which are needed in Session and
 * releases actor resources when session is closed.
 */
private[connection] class SessionActorBridge(loadBalancer: ActorRef,
                                             connectionPools: ActorRef,
                                             config: Configuration,
                                             debugMode: Boolean = false)(implicit system: ActorSystem) {

  def createRequestLifecycle(): ActorRef = {
    system.actorOf(Props(new RequestLifecycle(loadBalancer, connectionPools, config, debugMode)))
  }

  def closeSession(): Future[Boolean] = {
    gracefulStop(connectionPools, 1 second)
  }

}
