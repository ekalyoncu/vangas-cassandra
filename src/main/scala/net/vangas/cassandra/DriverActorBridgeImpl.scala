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

package net.vangas.cassandra

import akka.actor._

trait DriverActorBridge {

  def isSessionClosed(sessionId: Int): Boolean

  def activateSession(sessionId: Int)

  def closeSession(sessionId: Int)

  def setLoadBalancerAsDead()

  def isLoadBalancerAlive: Boolean

  def cleanAll()
}

class DriverActorBridgeImpl extends Extension with DriverActorBridge {

  @volatile private[cassandra] var loadBalancerAlive = true
  private[cassandra] val activeSessions = new java.util.concurrent.ConcurrentHashMap[Int, Boolean]()

  def isSessionClosed(sessionId: Int): Boolean = activeSessions.contains(sessionId)

  def activateSession(sessionId: Int) { activeSessions.put(sessionId, true) }

  def closeSession(sessionId: Int) { activeSessions.remove(sessionId) }

  def setLoadBalancerAsDead() { loadBalancerAlive = false }

  def isLoadBalancerAlive: Boolean = loadBalancerAlive

  def cleanAll() { activeSessions.clear() }

}

object DriverActorBridge extends ExtensionId[DriverActorBridgeImpl] with ExtensionIdProvider {

  override def lookup = DriverActorBridge

  override def createExtension(system: ExtendedActorSystem) = new DriverActorBridgeImpl()

  override def get(system: ActorSystem): DriverActorBridgeImpl = super.get(system)

}
