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

package net.vangas.cassandra.util

import java.util.{TimeZone, Calendar, UUID}

import org.joda.time.DateTime

// Methods in this class are taken from
// https://github.com/datastax/java-driver/blob/2.1/driver-core/src/main/java/com/datastax/driver/core/utils/UUIDs.java
object DateUtils {

  private val START_EPOCH = makeEpoch

  private def makeEpoch: Long = {
    val c: Calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"))
    c.set(Calendar.YEAR, 1582)
    c.set(Calendar.MONTH, Calendar.OCTOBER)
    c.set(Calendar.DAY_OF_MONTH, 15)
    c.set(Calendar.HOUR_OF_DAY, 0)
    c.set(Calendar.MINUTE, 0)
    c.set(Calendar.SECOND, 0)
    c.set(Calendar.MILLISECOND, 0)
    c.getTimeInMillis
  }

  def unixTimestamp(uuid: UUID): Long = (uuid.timestamp / 10000) + START_EPOCH

  def toDateTime(uuid: UUID): DateTime = {
    require(uuid.version() == 1, "please provide timeuuid which is type 1 uuid")
    new DateTime(unixTimestamp(uuid))
  }

}
