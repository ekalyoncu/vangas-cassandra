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

package net.vangas.cassandra.pimped

import net.vangas.cassandra.{ResultSet, Row}
import scala.collection.GenTraversableOnce
import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ExecutionContext, Future}
import scala.{PartialFunction => PF}

class FutureEx(underlying: Future[ResultSet]) {

  def one()(implicit executor: ExecutionContext): Future[Option[Row]] = underlying.map(_.headOption)

  def all()(implicit executor: ExecutionContext): Future[Seq[Row]] = underlying.map(_.toSeq)

  def mapRows[S, That](f: Row => S)
                     (implicit executor: ExecutionContext, bf: CanBuildFrom[Iterable[Row], S, That]): Future[That] = {
    underlying.map { _.map(f) }
  }

  def mapRow[S](f: Row => S)(implicit executor: ExecutionContext): Future[Option[S]] = {
    underlying.map{ _.headOption.map(f) }
  }

  def filterRows(f: Row => Boolean)(implicit executor: ExecutionContext): Future[Iterable[Row]] = {
    underlying.map { _.filter(f) }
  }

  def fMapRows[S, That](f: Row => GenTraversableOnce[S])
                      (implicit executor: ExecutionContext, bf: CanBuildFrom[Iterable[Row], S, That]): Future[That] = {
    underlying.map { _.flatMap(f) }
  }

  def fMapRow[S](f: Row => Option[S])(implicit executor: ExecutionContext): Future[Option[S]] = {
    underlying.map{ _.headOption.flatMap(f) }
  }

  def collectRows[S, That](f: PF[Row, S])
                   (implicit executor: ExecutionContext, bf: CanBuildFrom[Iterable[Row], S, That]): Future[That] = {
    underlying.map { _.collect(f) }
  }

  def collectRow[S](f: PF[Row, S])(implicit executor: ExecutionContext): Future[Option[S]] = {
    underlying.map{ _.headOption.collect(f) }
  }

}
