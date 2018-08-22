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

import Types.{Count, Volume, Probability, MLVector}
import scala.collection.mutable.PriorityQueue
import scala.math.{min, max, exp, log, pow, ceil}

import NodeLabelFunctions._
import LeafMapFunctions._

object HistogramUtilityFunctions {
  // WARNING: Approx. because it does not merge cells where removing one point
  // puts it below the splitting criterion
  def looL2ErrorApproxFromCells(totali : Count, cells : Iterable[(Volume, Count)]) : Double = {
    val total = 1.0 * totali
    cells.map {
      case (v : Volume, ci : Count) =>
        val c = 1.0*ci
        // (c/(v*total))^2 * v
      	val dtotsq = (c/total)*(c/total)/v
        // 1/total * (c-1)/(v*(total-1)) * c
        val douts = c*(c-1)/(v*(total - 1)*total)
        dtotsq - 2*douts
    }.sum
  }

  type PriorityFunction[H] = (NodeLabel, Count, Volume) => H

  // TODO: There must be a build-in version of this!
  def priorityQueueFrom[A](start : Seq[A])(implicit ord : Ordering[A]) : PriorityQueue[A] =
    PriorityQueue(start: _*)(ord)
}

import HistogramUtilityFunctions._

case class TailProbabilities(tree : SpatialTree, tails : LeafMap[Probability]) extends Serializable {
  def query(v : MLVector) : Double = {
    tails.query(tree.descendBox(v)) match {
      case (_, None)    => 0
      case (_, Some(p)) => p
    }
  }
}

case class Histogram(tree : SpatialTree, totalCount : Count, counts : LeafMap[Count]) extends Serializable {
  def density(v : MLVector) : Double = {
    counts.query(tree.descendBox(v)) match {
      case (_, None) => 0
      case (at, Some(c)) =>
        c / (totalCount * tree.volumeAt(at))
    }
  }

  def tailProbabilities() : TailProbabilities = {
    val quantiles = counts.toIterable.map {
      case (lab, c) => (lab, c/(totalCount * tree.volumeAt(lab)), c)
    }.toVector.sortBy {
      case (lab, d, p) => d
    }.toIterable.scanLeft((rootLabel, 0L)) {
      case ((_, c1), (lab, _, c2)) => (lab, c2 + c1)
    }.tail.map {
      case (lab, c) => (lab, c/(1.0*totalCount))
    }.toMap

    TailProbabilities(tree, fromNodeLabelMap(quantiles))
  }

  def truncation() : Truncation = counts.truncation

  def cells() : Iterable[(Volume, Count)] = counts.toIterable.map {
    case (lab : NodeLabel, c : Count) => (tree.volumeAt(lab), c)
  }

  def ncells() : Int = counts.size

  def looL2ErrorApprox() : Double = looL2ErrorApproxFromCells(totalCount, cells())

  def logLik() : Double = counts.toIterable.map {
    case (lab : NodeLabel, c : Count) => c*log(c/(totalCount * tree.volumeAt(lab)))
  }.sum

  def logPenalisedLik(taurec : Double) : Double =
    log(exp(taurec) - 1) - counts.toIterable.size*taurec + logLik()

  def backtrackWithNodes[H](prio : PriorityFunction[H])(implicit ord : Ordering[H]) : (Stream[(H, NodeLabel)], Stream[Histogram]) = {
    val start = counts.cherries(_+_).map {
      case (lab, c) => (prio(lab, c, tree.volumeAt(lab)), lab)
    }.toSeq

    val thisOuter : Histogram = this

    class HistogramBacktrackIterator extends Iterator[(H, NodeLabel, Histogram)] {
      val q = priorityQueueFrom(start)(ord.reverse.on(_._1)) // PriorityQueue(start: _*)(ord.reverse.on(_._1))
      var h = thisOuter
      override def hasNext : Boolean = !q.isEmpty
      override def next() : (H, NodeLabel, Histogram) = {
        val (p, lab : NodeLabel) = q.dequeue()

        val (cOpt, countsNew) = h.counts.mergeSubtreeCheckCherry(lab, _+_)

        val hnew = Histogram(h.tree, h.totalCount, countsNew)

        cOpt match {
          case None => ()
          case Some((cLab, cCnt)) => {
            q += ((prio(cLab, cCnt, tree.volumeAt(cLab)), cLab))
          }
        }

        h = hnew

        (p, lab, hnew)
      }
    }

    val intermediate = new HistogramBacktrackIterator().toStream
    (intermediate.map { case (prio, lab, _) => (prio, lab) }, this #:: intermediate.map { case (_, _, h) => h })
  }

  def backtrackToWithNodes[H](prio : PriorityFunction[H], hparent : Histogram)(implicit ord : Ordering[H])
      : (Stream[(H, NodeLabel)], Stream[Histogram]) = {

    class BacktrackToIterator extends Iterator[(H, NodeLabel, Histogram)] {
      // Split the histogram into one histogram per fringe-tree at a leaf in
      // hparent...
      val (initialTMP, splitsTMP, countMapsTMP) = (fringes(counts, hparent.truncation) match {
        case LeafMap(t, f) =>
          //... and for each one perform a backtrack
        f.toStream.zip(t.leaves.toStream).map {
            case (finner, r) =>
              // Some gymnastics to make all the laziness work correctly
              val (splits1, hs1) = Histogram(tree, totalCount, finner).backtrackWithNodes(prio)
              val (splits2, hs2) = (splits1 zip (hs1.tail)).takeWhile(_._1._2 != r.parent).unzip
              (finner, splits2.toIterator, hs2.map(_.counts).toIterator)
          }
      }).unzip3
      var current = initialTMP.toVector
      val splits = splitsTMP.toVector
      val countMaps = countMapsTMP.toVector

      // val pqInit = splitStream.toStream.zipWithIndex.filter(_.hasNext).map {
      //   case (x, i) =>
      //     val (p1, lab1) = x.next()
      //     (i, p1, lab1)
      // }

      //PriorityQueue(pqInit: _*)(ord.reverse.on(_._2))
      val q : PriorityQueue[(Int, H, NodeLabel)] =
        priorityQueueFrom(splits.toStream.zipWithIndex.filter(_._1.hasNext).map {
                            case (x, i) =>
                              val (p1, lab1) = x.next()
                              (i, p1, lab1)
                          })(ord.reverse.on(_._2))

      override def hasNext() : Boolean =
        !q.isEmpty
      // { if(q.isEmpty) {
      //     assert(countMaps.forall(!_.hasNext))
      //     false
      //   } else true
      // }

      override def next() : (H, NodeLabel, Histogram) = {
        val (i, p, lab) = q.dequeue()

        if(!splits(i).isEmpty) {
          splits(i).next() match {
            case (pNew, labNew) =>
              q += ((i, pNew, labNew))
          }
        }

        current = current.updated(i, countMaps(i).next)

        (p, lab, Histogram(tree, totalCount, concatLeafMaps(current)))
      }
    }

    val intermediate = (new BacktrackToIterator()).toStream
    (intermediate.map { case (prio, lab, _) => (prio, lab) }, this #:: intermediate.map { case (_, _, h) => h })
  }

  def backtrackNodes[H](prio : PriorityFunction[H])(implicit ord : Ordering[H])
      : Stream[NodeLabel] =
    backtrackWithNodes(prio)(ord)._1.map(_._2)

  def backtrack[H](prio : PriorityFunction[H])(implicit ord : Ordering[H])
      : Stream[Histogram] =
    backtrackWithNodes(prio)(ord)._2

  def backtrackToNodes[H](prio : PriorityFunction[H], hparent : Histogram)(implicit ord : Ordering[H]) : Stream[NodeLabel] =
    backtrackToWithNodes(prio, hparent)(ord)._1.map(_._2)

  def backtrackTo[H](prio : PriorityFunction[H], hparent : Histogram)(implicit ord : Ordering[H]) : Stream[Histogram] =
    backtrackToWithNodes(prio, hparent)(ord)._2
}
