# Vangas

This is a Scala driver for [Apache Cassandra](https://github.com/apache/cassandra). It is fully asynchronous and non-blocking based on [Akka-IO](http://doc.akka.io/docs/akka/2.3.6/scala/io.html) and [Akka-Actors](http://doc.akka.io/docs/akka/2.3.6/scala/actors.html).
This driver is not a wrapper on [cassandra-java-driver](https://github.com/datastax/java-driver).
**Currently, Vangas supports only native-protocol-v3**.

## Features

* Non-blocking IO
* Query
* Query value bindings
* PreparedStatements
* Execute
* Retry of unprepared queries
* ConnectionPool
* Retry of failed queries if Connection reaches max concurrent request limit which is 128 (limitation of current cassandra protocol)
* [Pagination](http://www.datastax.com/documentation/cql/3.0/cql/cql_using/paging_c.html) using token function
* Most of [data-types](http://www.datastax.com/documentation/cql/3.0/cql/cql_reference/cql_data_types_c.html) except Tuples and Custom types

## TODOs
* Batch queries
* Authentication
* Value bindings by name
* Compression support
* Listening server-side events
* Add more (to a certain limit) connections when current connections reach their max concurrent request limit.
* Tuples and custom data-types
* Native-protocol-v2 support

## Getting started

### Create CassandraClient
There should be one CassandraClient per cluster. There is an ActorSystem per CassandraClient.

```scala
val client = new CassandraClient(Seq("localhost"))
```

### Session
Sessions are the containers that keep connections to the Cassandra cluster.  Each session object serves for one keyspace. There is an actor associated with each session object which handles routing of requests to the connection actors which send them to a cassandra node.

Actor associated with the session has connection pool which holds connection actors' reference. Requests are routed to the connections in round-robin fashion.

#### Connection
Connection is an actor which keeps the TCP connection to a cassandra node. All CQL commands are sent to the node from this actor. This actor is not exposed to the driver clients.

#### Create new Session

```scala
val session = client.newSession("KEY_SPACE")
```
> WARNING: Don't change the keyspace via USE command in a session. If you need to execute queries on different keyspace, create new session instance.

> Don't do this <code>session.execute("USE new_keyspace")</code>

#### Execute simple query

```scala
val results: Future[ResultSet] = session.execute("SELECT * FROM users")
results.onSuccess{ case rs =>
  rs.foreach{ row =>
    row.get[Int](0) //gets the first column which is Int
  }
}
```

#### Pimped response
There are syntactic sugars which removes boilerplate codes while processing ResultSets.
```scala
import net.vangas.cassandra.pimped._
```

```scala
session.execute("SELECT * FROM users").mapRows { row =>
  //convert to your case class
}
```

#### Bind values

```scala
val results: Future[ResultSet] = session.execute("SELECT * FROM users WHERE id = ?", 123)
```

#### PreparedStatement
```scala
val prepare: Future[PreparedStatement] = session.prepare("SELECT * FROM users WHERE id = ?")
val results: Future[ResultSet] = prepare.flatMap{ prepared => session.execute(prepared.bind("123"))}
```

### Close Session
You cannot reuse closed session.
```scala
session.close()
```

### Close CassandraClient
You cannot reuse closed CassandraClient.
```scala
client.close()
```

## Build
In order to build this project, you need a running cassandra instance locally. We have integration tests which need running cassandra instance. You can use [ccm](http://github.com/pcmanus/ccm) which makes it easy to create and teste cassandra cluster locally.

## Releases
Vangas is not released yet because it's still under testing. But feel free to evaluate and create pull requests.

## License
Copyright (C) 2014 Egemen Kalyoncu

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
