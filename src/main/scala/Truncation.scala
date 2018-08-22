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

import Types._

import NodeLabelFunctions._

// Leaf-labelled finite (truncated) binary trees

// TODO: Can we make efficient splices part of Truncation instead?
case class Subset(lower : Int, upper : Int) extends Serializable {
  def size() : Int = upper - lower
  def isEmpty() : Boolean = size() == 0
  def midpoint() : Int = (upper + lower)/2 // Should round down
  def sliceLow(x : Int) : Subset = Subset(max(x, lower), max(x, upper))
  def sliceHigh(x : Int) : Subset = Subset(min(x, lower), min(x, upper))
  def normalise() : Subset = if(size() == 0) Subset(0,0) else this
}

object BinarySearchFunctions {

  // WARNING: expects f(x_1), f(x_2), ... to initially be all false then all true
  def binarySearchWithin[A](f : A => Boolean)(xs : Vector[A], ss : Subset) : Int = {
    val res = Stream.iterate(ss) {
      case s =>
        if(f(xs(s.midpoint)))
          s.sliceHigh(s.midpoint)
        else
          s.sliceLow(s.midpoint)
    }.dropWhile(_.size > 1)

    // TODO: Make this nicer
    if(res.head.size == 0)
      ss.upper
    else
      res.tail.head.upper
  }

  def binarySearch[A](f : A => Boolean)(xs : Vector[A]) : Int = {
    binarySearchWithin(f)(xs, Subset(0, xs.size))
  }

  class PadIterator[A](iter : Iterator[A], atInit : Int, atEnd : Int) extends Iterator[A]  {
    // WARNING: Just crashes if iter is empty (but also has undefined semantics in that case!)
    var last = iter.next()
    var initCount = atInit+1
    var endCount = atEnd
    override def hasNext() : Boolean =
      iter.hasNext || endCount > 0
    override def next() : A = {
      // println(initCount)
      // println(endCount)
      if(initCount > 0) {
        initCount = initCount - 1
        last
      } else if(iter.hasNext) {
        last = iter.next()
        last
      } else {
        endCount = endCount - 1
        last
      }
    }
  }

  def padIterator[A](iter : Iterator[A], atInit : Int, atEnd : Int) : Iterator[A] =
    new PadIterator(iter, atInit, atEnd)
}

import BinarySearchFunctions._

case class Truncation(leaves : Vector[NodeLabel]) extends Serializable {
  // TODO: Make this a more efficent binary search
  def subtreeWithin(at : NodeLabel, within : Subset) : Subset = {
    val low = binarySearchWithin((x : NodeLabel) => !isStrictLeftOf(x, at))(leaves, within)
    val high = binarySearchWithin((x : NodeLabel) => x != at && !isDescendantOf(x, at))(leaves, within.sliceLow(low))
    Subset(low, high).normalise
    // }
  }

  def restrict(ss : Subset) : Truncation = Truncation(leaves.slice(ss.lower, ss.upper))

  def allNodes() : Subset = Subset(0, leaves.length)

  def subtree(at : NodeLabel) : Subset = subtreeWithin(at, allNodes())

  def viewSubtree(at : NodeLabel, margin : Int) : Vector[NodeLabel] = {
    val ss = subtree(at)
    leaves.slice(ss.lower-margin, ss.upper+margin)
  }

  def cherryAtIdx(idx : Int) : Option[Vector[Int]] = {
    val x = leaves(idx)
    if(x.isLeft) {
      if(idx == leaves.length-1) {
        some(Vector(idx))
      } else {
        val r = leaves(idx+1)
        if(r == x.sibling) some(Vector(idx, idx+1))
        else if(!inRightSubtreeOf(r, x.parent)) some(Vector(idx))
        else none()
      }
    } else {
      if(idx == 0) {
        some(Vector(idx))
      } else {
        val l = leaves(idx-1)
        if(l == x.sibling) some(Vector(idx-1, idx))
        else if(!inLeftSubtreeOf(l, x.parent)) some(Vector(idx))
        else none()
      }
    }
  }

  def hasAsCherry(lab : NodeLabel) : Boolean = {
    val ss = subtree(lab)
      ss.size match {
        case 1 => leaves(ss.lower) == lab.left || leaves(ss.lower)   == lab.right
        case 2 => leaves(ss.lower) == lab.left && leaves(ss.lower+1) == lab.right
        case n => false
      }
  }

  def cherries() : Iterator[Vector[Int]] = {
    if(leaves.length == 0) return Iterator.empty
    else if(leaves.length == 1) return List(Vector(0)).toIterator
    else {
      val inner = (0 until leaves.length).sliding(2).flatMap {
        case x =>
          // TODO: Move this out
          if(x.length == 1) {
            List(Vector(x(0)))
          } else {
            val l = leaves(x(0))
            val r = leaves(x(1))
            if(l.sibling == r) {
              List(Vector(x(0), x(1)))
            } else {
              val lonerl =
                if(l.isLeft && !inRightSubtreeOf(r, l.parent)) List(Vector(x(0)))
                else                                           List()
              val lonerr =
                if(r.isRight && !inLeftSubtreeOf(l, r.parent)) List(Vector(x(1)))
                else                                           List()
              lonerl ++ lonerr
            }
          }
      }.toIterator

      val leftcorner  = (if(leaves.head.isRight) List(Vector(0)) else List()).toIterator
      val rightcorner = (if(leaves.last.isLeft) List(Vector(leaves.length-1)) else List()).toIterator

      leftcorner ++ inner ++ rightcorner
    }
  }

  def slice(ss : Subset) : Iterator[NodeLabel] = leaves.slice(ss.lower, ss.upper).toIterator

  def descend(labs : Walk) : Stream[Subset] =
    labs.scanLeft(allNodes()) {
      case (ss, lab) => subtreeWithin(lab, ss)
    }.tail

  // TODO: Optimise?
  // TODO: Figure out better error handling in case Walk starts in empty subtree!
  def descendUntilLeafWhere(labs : Walk) : (NodeLabel, Subset) =
    labs.zip(descend(labs)).takeWhile{case (_, ss) => ss.size > 0}.last

  def descendUntilLeaf(labs : Walk) : NodeLabel =
    descendUntilLeafWhere(labs)._1

  // NOTE: Computes the minimal tree containing the leaves of the truncation
  def minimalCompletionNodes() : Stream[(NodeLabel, Option[Int])] = {
    if(leaves.length == 0) {
      Stream((rootLabel, none()))
    } else if(leaves.length == 1 && leaves(0) == rootLabel) {
      Stream((rootLabel, some(0)))
    } else {
      // If necessary add a left/right-most leaf
      val firstLeaf = leaves.head
      val lastLeaf = leaves.last
      val llim = firstLeaf.truncate(firstLeaf.initialLefts)
      val rlim = lastLeaf.truncate(lastLeaf.initialRights)
      val l : Stream[(NodeLabel, Option[Int])] = if(llim != firstLeaf) Stream((llim.left, none())) else Stream.empty
      val r : Stream[(NodeLabel, Option[Int])] = if(rlim != lastLeaf) Stream((rlim.right, none())) else Stream.empty
      val c : Stream[(NodeLabel, Option[Int])] = leaves.toStream.zip(0 until leaves.length).map(x => (x._1, some(x._2)))

      val leavesWidened : Stream[(NodeLabel, Option[Int])] = l #::: c #::: r

      val leavesFilled : Stream[(NodeLabel, Option[Int])] = leavesWidened.sliding(2).flatMap {
        case ((llab, _) #:: (rlab, rval) #:: Stream.Empty) =>
          if(llab == rlab.sibling) {
            Stream((rlab, rval))
          } else {
            val j = join(llab, rlab)
            val rightFilled = pathLeftClosed(llab, j.left).
              filter(_.isLeft).
              map(x => (x.sibling, none()))


            val leftFilled = pathRightClosed(j.right, rlab).
              filter(_.isRight).
              map(x => (x.sibling, none()))

            rightFilled #::: leftFilled #::: Stream((rlab, rval))
          }
      }.toStream

      leavesWidened.head #:: leavesFilled
    }
  }

  def minimalCompletion() : Truncation =
    Truncation(minimalCompletionNodes.map(_._1).toVector)
}

object TruncationFunctions {
  // WARNING: Currently does not check whether leaves can be a set of leaves
  // (i.e. that it does not contain ancestor-descendant pairs)
  def fromLeafSet(leaves : Iterable[NodeLabel]) : Truncation =
    Truncation(leaves.toVector.sorted(leftRightOrd))

  def rootTruncation() : Truncation = Truncation(Vector(rootLabel))
}
