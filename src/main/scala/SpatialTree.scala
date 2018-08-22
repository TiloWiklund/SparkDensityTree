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

import scala.math.{min, max, exp, log, pow, ceil}

import org.apache.spark.rdd._
import org.apache.spark.rdd.PairRDDFunctions._

import Types.MLVector

import UnfoldTreeFunctions._
import NodeLabelFunctions._
import RectangleFunctions._

// TODO: (IMPORTANT) axisAt etc should be replaced by an abstract left/right
// test function to accomodate non-axis-alinged strategies
abstract class SpatialTree extends Serializable {
  def rootCell : Rectangle

  def dimension() : Int = this.rootCell.dimension
  def volumeTotal() : Double = this.rootCell.volume

  def volumeAt(at : NodeLabel) : Double =
    this.cellAt(at).volume

  def axisAt(at : NodeLabel) : Int

  def cellAt(at : NodeLabel) : Rectangle

  def cellAtCached() : CachedUnfoldTree[Rectangle] =
    unfoldTreeCached(rootCell)((lab, box) => box.lower(axisAt(lab.parent)),
                               (lab, box) => box.upper(axisAt(lab.parent)))

    def descendBoxPrime(point : MLVector) : Stream[(NodeLabel, Rectangle)]

  def descendBox(point : MLVector) : Stream[NodeLabel] = descendBoxPrime(point).map(_._1)
}

case class WidestSplitTree(rootCellM : Rectangle) extends SpatialTree {
  override def rootCell = rootCellM

  override def volumeAt(at : NodeLabel) : Double =
    rootCellM.volume / pow(2, at.depth)

  // NOTE: Emulates unfold, since unfold apparently only exists in Scalaz...
  val splits : Stream[Int] = Stream.iterate((0, rootCellM.widths)) {
    case (_, ws) =>
      val i = ws.zipWithIndex.maxBy(_._1)._2
      (i, ws.updated(i, ws(i)/2))
  }.map(_._1).tail

  //TODO: Optimise
  override def axisAt(at : NodeLabel) : Int = splits(at.depth)

  override def cellAt(at : NodeLabel) : Rectangle = (at.lefts zip splits).foldLeft(rootCell) {
    case (cell, (l, i)) =>
      if(l) cell.lower(i) else cell.upper(i)
  }

    // TODO: Make a more efficient implementation of this!
    // override def cellAtCached() : CachedUnfoldTree[Rectangle]

  override def descendBoxPrime(point : MLVector) : Stream[(NodeLabel, Rectangle)] =
    splits.scanLeft((rootLabel, rootCell)) {
      case ((lab, box), along) =>
        if(box.isStrictLeftOfCentre(along, point))
          (lab.left, box.lower(along))
          else
            (lab.right, box.upper(along))
    }
}

  // TODO: Can we figure out some clever way to do memoisation/caching?
case class UniformSplitTree(rootCellM : Rectangle) extends SpatialTree {
  override def rootCell = rootCellM

  // def dimension() : Int = rootCell.dimension
  // def volumeTotal() : Double = rootCell.volume

  override def volumeAt(at : NodeLabel) : Double =
    rootCellM.volume / pow(2, at.depth)

  override def axisAt(at : NodeLabel) : Int =
    at.depth % dimension()

  override def cellAt(at : NodeLabel) : Rectangle =
    unfoldTree(rootCell)((lab, box) => box.lower(axisAt(lab.parent)),
                         (lab, box) => box.upper(axisAt(lab.parent)))(at)

  override def descendBoxPrime(point : MLVector) : Stream[(NodeLabel, Rectangle)] = {
    def step(lab : NodeLabel, box : Rectangle) : (NodeLabel, Rectangle) = {
      val along = axisAt(lab)

        if(box.isStrictLeftOfCentre(along, point))
          (lab.left, box.lower(along))
        else
          (lab.right, box.upper(along))
    }

    Stream.iterate((rootLabel, rootCell))(Function.tupled(step))
  }
}

object SpatialTreeFunctions {
  // def spatialTreeRootedAt(rootCell : Rectangle) : SpatialTree = SpatialTree(rootCell)
  def uniformTreeRootedAt(rootCell : Rectangle) : SpatialTree = UniformSplitTree(rootCell)
  def widestSideTreeRootedAt(rootCell : Rectangle) : SpatialTree = WidestSplitTree(rootCell)

  type PartitioningStrategy = RDD[MLVector] => SpatialTree

  val splitAlongWidest : PartitioningStrategy = (points => widestSideTreeRootedAt(boundingBox(points)))
  val splitUniformly : PartitioningStrategy = (points => uniformTreeRootedAt(boundingBox(points)))
}

import SpatialTreeFunctions._
