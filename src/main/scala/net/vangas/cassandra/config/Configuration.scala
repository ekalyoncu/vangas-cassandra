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

package net.vangas.cassandra.config

import net.vangas.cassandra.loadbalancing.{RoundRobinLoadBalancingPolicy, LoadBalancingPolicy}

import scala.concurrent.duration._

case class Configuration(port: Int = 9042,
                         connectionsPerNode: Int = 2,
                         connectionTimeout: FiniteDuration = 10.seconds,
                         queryTimeout: FiniteDuration = 1.second,
                         loadBalancingPolicy: LoadBalancingPolicy = new RoundRobinLoadBalancingPolicy,
                         queryConfig: QueryConfig = QueryConfig())
