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

import scala.collection.mutable.HashMap

import Types._

import PartitionedFunctions._
import LeafMapFunctions._
import SpatialTreeFunctions._
import TruncationFunctions._

object SplitEstimatorFunctions {
  type SplitLimits = (Volume, Count) => (Int, Volume, Count) => Boolean

  type StopRule = (Volume, Count, Iterable[(Double, Count)]) => Boolean

  def noEarlyStop(tv : Volume, tc : Count, cs : Iterable[(Double, Count)]) : Boolean = false
  def boundLeaves(bound : Int) : StopRule = (tv : Volume, tc : Count, cs : Iterable[(Double, Count)]) => cs.size >= bound

  // TODO: Ensure that the resulting NodeLabel is a refinement of tree
  // TODO: !!! Figure out if we need to explicitly persist stuff
  def splitAndCountFrom(  tree : SpatialTree,
                         trunc : Truncation,
                        points : RDD[MLVector],
                        limits : SplitLimits,
                          stop : StopRule ) : Map[NodeLabel, Count] = {
    var accCounts : HashMap[NodeLabel, Count] = HashMap()
    var boxCache = tree.cellAtCached()
    var partitioned : Partitioned[MLVector] = partitionPoints(tree, trunc, points)

    val  totalCount = points.count
    val totalVolume = tree.volumeTotal
    val   splitRule = limits(totalVolume, totalCount)

    // Apparently the block inside a do-while loop is out of scope in the
    // stopping criterion, I love Scala so much...
    var scalaplease : Map[NodeLabel, Count] = null
    do {
      val (doSplit, dontSplit) = partitioned.splittable((lab, c) => splitRule(lab.depth, tree.volumeAt(lab), c))
      scalaplease = doSplit

      accCounts ++= dontSplit
      boxCache = boxCache.recache(doSplit.keySet)

      val splitPlanes : Map[NodeLabel,(Int,Double)] = boxCache.cache.map {
        case (l,v) =>
          (l, (tree.axisAt(l), boxCache(l).centre(tree.axisAt(l))))
      }

      partitioned = partitioned.subset(doSplit.keySet).split {
        case (lab, x) =>
          val (along, centre) = splitPlanes(lab)
          if(x(along) <= centre) lab.left else lab.right
      }
    } while(!scalaplease.isEmpty && !stop(totalVolume, totalCount, accCounts.toIterable.map {case (l, c) => (tree.volumeAt(l), c)}))

    accCounts.toMap
  }

  def histogramFrom(tree : SpatialTree,
                    trunc : Truncation,
                    points : RDD[MLVector],
                    limits : SplitLimits,
                    stop : StopRule) : Histogram = {
    val counts = splitAndCountFrom(tree, trunc, points, limits, stop)
    val totalCount = counts.values.sum
    Histogram(tree, totalCount, fromNodeLabelMap(counts))
  }

  def histogramStartingWith(h : Histogram,
                            points : RDD[MLVector],
                            limits : SplitLimits,
                            stop : StopRule = noEarlyStop) : Histogram =
    histogramFrom(h.tree, h.truncation, points, limits, stop)

  def histogram(points : RDD[MLVector],
                limits : SplitLimits,
                stop : StopRule = noEarlyStop,
                partitioningStrategy : PartitioningStrategy = splitAlongWidest) : Histogram =
    histogramFrom(partitioningStrategy(points), rootTruncation, points, limits, stop)
}

import SplitEstimatorFunctions._
