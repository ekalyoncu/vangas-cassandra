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

package net.vangas.cassandra.integration

import net.vangas.cassandra.CCMSupport
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec}

class RoundRobinLoadBalancingIntegrationSpec extends FunSpec with CCMSupport with BeforeAndAfterAll with BeforeAndAfter {

  val cluster = "rrlb_cluster"
  val keyspace = "rrlb_ks1"



}
