/*
 * Copyright (C) 2014 Egemen Kalyoncu
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

import akka.actor.{Terminated, ActorLogging, ActorRef, Actor}

class ForwardingActor(underlying: ActorRef) extends Actor with ActorLogging {

  context.watch(underlying)

  override def preStart(): Unit = {
    log.info("Starting forwarding actor. Underlying: {}", underlying.path)
    underlying ! "Started"
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info("Restarting forwarding actor. Underlying: {}", underlying.path)
    underlying ! "Restarted"
    super.postRestart(reason)
  }

  override def postStop(): Unit = {
    log.info("Stopped forwarding actor. Underlying: {}", underlying.path)
    underlying ! "Closed"
  }

  def receive = {
    case Terminated(actor) =>
      if (actor == underlying) {
        log.warning("Underlying actor is dead! Killing itself...")
        context stop self
      }

    case msg =>
      val snd = sender()
      log.info(s"Forwarding $msg from $snd to $underlying")
      underlying.!(msg)(snd)

  }
}
