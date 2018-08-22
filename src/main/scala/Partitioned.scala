/**************************************************************************
 * Copyright 2017 Tilo Wiklund
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
 **************************************************************************/

package co.wiklund.disthist

import org.apache.spark.rdd._
import org.apache.spark.rdd.PairRDDFunctions._

import Types._

// TODO: This should maybe be parameterised over Count/countByKey as well
case class Partitioned[A](points : RDD[(BigInt, A)]) extends Serializable {
  def splittable(shouldSplit : (NodeLabel, Count) => Boolean) : (Map[NodeLabel, Count], Map[NodeLabel, Count]) = {
    // TODO: Why is keyBy needed, it should be noop here?!?
    // NOTE: This is needed to make things work in SparkREPL/DB, and maybe save some space if Spark/Scala are dumb
    points.keyBy(_._1).countByKey().toMap.map(x => (NodeLabel(x._1), x._2)).partition(Function.tupled(shouldSplit))
  }

  def subset(labs : Set[NodeLabel]) : Partitioned[A] = {
    val actualLabs = labs.map(_.lab)
    Partitioned(points.filter(x => actualLabs(x._1)))
  }

  def split(rule : (NodeLabel, A) => NodeLabel) : Partitioned[A] =
    Partitioned(points.map{case(l, v) => (rule(NodeLabel(l), v).lab, v)})

  def count() : Long = points.count()
}

object PartitionedFunctions {
  def partitionPoints(tree : SpatialTree, trunc : Truncation, points : RDD[MLVector]) : Partitioned[MLVector] =
    Partitioned(points.map(x => (trunc.descendUntilLeaf(tree.descendBox(x)).lab, x)))
}

import PartitionedFunctions._
