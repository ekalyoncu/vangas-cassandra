package net.vangas.cassandra.config

import net.vangas.cassandra.{QueryParameters, ConsistencyLevel}
import net.vangas.cassandra.ConsistencyLevel.ConsistencyLevel

case class QueryConfig(consistency: ConsistencyLevel = ConsistencyLevel.ONE,
                       pageSize: Int = -1, //Currently driver doesn't support auto-paging
                       serialConsistency: ConsistencyLevel = ConsistencyLevel.SERIAL) {

  private[cassandra] def toQueryParameters(values: Seq[_] = Seq.empty) =
    new QueryParameters(consistency, values, false, pageSize, None, serialConsistency)
}
