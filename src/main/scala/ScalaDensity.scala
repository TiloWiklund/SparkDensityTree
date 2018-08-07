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
import scala.math.{min, max, exp, log, pow, ceil}
import scala.math.BigInt._
import scala.math.PartialOrdering

import scala.collection.mutable.{ HashMap, PriorityQueue }
import scala.collection.mutable.{ Set => MSet, Map => MMap }
import scala.collection.{mutable, immutable}
import scala.collection.immutable.{Set, Map}
import scala.reflect.ClassTag

import org.apache.spark.rdd._
import org.apache.spark.rdd.PairRDDFunctions._

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{ Vector => MLVector, _ }

// import scala.util.Sorting

object ScalaDensity {

  // Axis parallel (bounding) boxes

  type Axis = Int
  type Intercept = Double
  type Volume = Double

  case class Rectangle(low : Vector[Double], high : Vector[Double]) extends Serializable {
    def factorise() : Iterator[(Double, Double)] = low.toIterator.zip(high.toIterator)
    override def toString = factorise().mkString("x")

    def dimension() = high.length

    def centre(along : Axis) : Double =
      (high(along) + low(along))/2

    def split(along : Axis) : (Rectangle, Rectangle) = split(along, centre(along))

    def split(along : Axis, intercept : Intercept) : (Rectangle, Rectangle) = {
      val c = min(max(intercept, low(along)), high(along))
      (Rectangle(low,                   high.updated(along, c)),
       Rectangle(low.updated(along, c), high                  ))
    }

    def lower(along : Axis) : Rectangle = split(along)._1
    def lower(along : Axis, intercept : Intercept) : Rectangle =
      split(along, intercept)._1

    def upper(along : Axis) : Rectangle = split(along)._2
    def upper(along : Axis, intercept : Intercept) : Rectangle =
      split(along, intercept)._2

    def widths() : Vector[Double] = (high, low).zipped map (_-_)

    def volume() : Volume = widths() reduce (_*_)

    def contains(v : MLVector) =
      v.toArray.toVector.zip(high.zip(low)).forall { case (c, (h, l)) => h >= c && c >= l }

    def isStrictLeftOfCentre(along : Axis, v : MLVector) : Boolean =
      v(along) <= centre(along)

    def isStrictRightOfCentre(along : Axis, v : MLVector) : Boolean =
      v(along) >  centre(along)
  }

  def hull(b1 : Rectangle, b2 : Rectangle) : Rectangle =
    Rectangle( ( b1.low, b2.low ).zipped map min,
               (b1.high, b2.high).zipped map max )

  def point(v : MLVector) : Rectangle =
    Rectangle(v.toArray.toVector, v.toArray.toVector)

  def boundingBox(vs : RDD[MLVector]) : Rectangle =
    vs.map(point(_)).reduce(hull)

  // (The) infinite binary tree

  type Depth = Int

  case class NodeLabel(lab : BigInt) extends Serializable {
    private val rootLabel : BigInt = 1

    def    left() : NodeLabel = NodeLabel(2*lab)
    def   right() : NodeLabel = NodeLabel(2*lab + 1)
    def isRight() : Boolean   =  lab.testBit(0)
    def  isLeft() : Boolean   = !lab.testBit(0)

    // WARNING: Not checking to make sure we're sufficiently deep
    def ancestor(level : Int) : NodeLabel = NodeLabel(lab >> level)
    def parent() : NodeLabel = ancestor(1)
    def sibling() : NodeLabel =
      if(isLeft()) parent().right else parent().left

    def children() : Set[NodeLabel] =
      Set(left(), right())

    def ancestors() : Stream[NodeLabel] =
      Stream.iterate(this)({_.parent}).takeWhile(_.lab >= rootLabel).tail

    def depth() : Depth = lab.bitLength - 1
    def truncate(toDepth : Depth) : NodeLabel = {
      val fromDepth = depth()
      if(toDepth >= fromDepth) this else ancestor(fromDepth - toDepth)
    }

    def lefts() : Stream[Boolean] = ((0 to depth()) map (lab.testBit(_))).toStream
    def rights() : Stream[Boolean] = ((0 to depth()) map (!lab.testBit(_))).toStream

    // Auxiliary stuff
    def invert() : NodeLabel =
      NodeLabel(lab ^ ((1 << (lab.bitLength-1)) - 1))
    def initialLefts() : Int =
      lab.bitLength - lab.clearBit(lab.bitLength-1).bitLength - 1
    def initialRights() : Int =
      invert().initialLefts

    // TODO: Rewrite this in terms of lefts()
    def mrsName() : String = (this #:: ancestors).reverse.flatMap {
      case lab =>
        if(lab == NodeLabel(rootLabel)) "X"
        else if(lab.isLeft) "L"
        else "R"
    }.mkString
  }

  val rootLabel : NodeLabel = NodeLabel(1)

  def join(a : NodeLabel, b : NodeLabel) : NodeLabel = {
    val d = min(a.depth, b.depth)
    val aT = a.truncate(d)
    val bT = b.truncate(d)
    aT.ancestor((aT.lab ^ bT.lab).bitLength)
  }

  object leftRightOrd extends Ordering[NodeLabel] {
    def compare(a : NodeLabel, b : NodeLabel) = {
      if(a == b) 0
      else {
        val j = join(a, b)
        if(inLeftSubtreeOf(a, j) || inRightSubtreeOf(b, j)) -1
        else 1
      }
    }
  }

  object ancestralOrder extends PartialOrdering[NodeLabel] {
    def tryCompare(a : NodeLabel, b : NodeLabel) = {
      if(a == b) some(0)
      else if(a.lab < b.lab) {
        if(b.truncate(a.depth) == a) some(-1)
        else                         none()
      }
      else /* if(a.lab > b.lab) */ {
        if(a.truncate(b.depth) == b) some( 1)
        else                         none()
      }
    }

    def lteq(a : NodeLabel, b : NodeLabel) = {
      this.tryCompare(a, b) match {
        case Some(c) => c <= 0
        case None    => false
      }
    }
  }

  def isAncestorOf(a : NodeLabel, b : NodeLabel) : Boolean =
    a.lab < b.lab && b.truncate(a.depth) == a
    // a.lab < b.lab && leftRightOrd.compare(a, b) == 0

  def isDescendantOf(a : NodeLabel, b : NodeLabel) : Boolean =
    isAncestorOf(b, a)

  def isLeftOf(a : NodeLabel, b : NodeLabel) : Boolean =
    leftRightOrd.compare(a, b) < 0

  def isStrictLeftOf(a : NodeLabel, b : NodeLabel) : Boolean =
    a.truncate(b.depth).lab < b.truncate(a.depth).lab
    // leftRightOrd.compare(a, b) < 0

  def isRightOf(a : NodeLabel, b : NodeLabel) : Boolean =
    leftRightOrd.compare(a, b) > 0

  def isStrictRightOf(a : NodeLabel, b : NodeLabel) : Boolean =
    isStrictLeftOf(b, a)

  def inLeftSubtreeOf(a : NodeLabel, b : NodeLabel) : Boolean =
    a == b.left || isAncestorOf(b.left, a)

  def inRightSubtreeOf(a : NodeLabel, b : NodeLabel) : Boolean =
    a == b.right || isAncestorOf(b.right, a)

  def adjacent(a : NodeLabel, b : NodeLabel) : Boolean =
    a.parent == b || b.parent == a

  def pathLeftClosed(from : NodeLabel, to : NodeLabel) : Stream[NodeLabel] =
    if(from == to) Stream.empty else from #:: path(from, to)

  def pathRightClosed(from : NodeLabel, to : NodeLabel) : Stream[NodeLabel] =
    if(from == to) Stream.empty else path(from, to) #::: Stream(to)

  def path(from : NodeLabel, to : NodeLabel) : Stream[NodeLabel] = {
    if(from == to) {
      Stream.empty
    } else if(isDescendantOf(from, to)) {
      from.ancestors.takeWhile(_.lab > to.lab)
    } else if(isAncestorOf(from, to)) {
      (from.depth + 1 until to.depth).toStream.map(to.truncate(_))
    } else {
      val j = join(from, to)
      path(from, j) #::: Stream(j) #::: path(j, to)
    }
  }

  def unfoldTree[A](base : A)(left : (NodeLabel, A) => A, right : (NodeLabel, A) => A)(lab : NodeLabel) : A = {
    if(lab == rootLabel) {
      base
    } else if(lab.isLeft) {
      left(lab, unfoldTree(base)(left, right)(lab.parent))
    } else {
      right(lab, unfoldTree(base)(left, right)(lab.parent))
    }
  }

  case class CachedUnfoldTree[A]( base : A,
                                 cache : Map[NodeLabel, A],
                                  left : (NodeLabel, A) => A,
                                 right : (NodeLabel, A) => A) extends Serializable {
    def apply(lab : NodeLabel) : A = {
      val inCache : NodeLabel = (lab.ancestors.dropWhile(!cache.isDefinedAt(_)) #::: Stream(rootLabel)).head
      val startWith : A = if(inCache == rootLabel) base else cache(inCache)

      // println(lab)

      // TODO: Make this nicer
      var clab = inCache
      var result = startWith
      while(clab != lab) {
        val goLeft = inLeftSubtreeOf(lab, clab)
        if(goLeft) {
          clab = clab.left
          result = left(clab, result)
        } else {
          clab = clab.right
          result = right(clab, result)
        }
      }
      result
    }

    def recache(at : Iterable[NodeLabel]) : CachedUnfoldTree[A] =
      CachedUnfoldTree(base,
                       at.map(lab => (lab, this(lab))).toMap,
                       left,
                       right)
  }

  def unfoldTreeCached[A](base : A)(left : (NodeLabel, A) => A, right : (NodeLabel, A) => A) : CachedUnfoldTree[A] =
    CachedUnfoldTree(base, Map.empty, left, right)

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

  type Walk = Stream[NodeLabel]

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
    def descendUntilLeafWhere(labs : Walk) : (NodeLabel, Subset) =
      labs.zip(descend(labs)).dropWhile{case (_, ss) => ss.size > 1}.head

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

  // WARNING: Currently does not check whether leaves can be a set of leaves
  // (i.e. that it does not contain ancestor-descendant pairs)
  def fromLeafSet(leaves : Iterable[NodeLabel]) : Truncation =
    Truncation(leaves.toVector.sorted(leftRightOrd))

  def rootTruncation() : Truncation = Truncation(Vector(rootLabel))

  def some[A](a : A) : Option[A] = Some(a)
  def none[A]() : Option[A] = None

  case class LeafMap[A:ClassTag](truncation : Truncation, vals : Vector[A]) extends Serializable {
    // TODO: Optimise?
    def query(labs : Walk) : (NodeLabel, Option[A]) = {
      val (at, ss) = truncation.descendUntilLeafWhere(labs)
      if(ss.isEmpty) (at, none()) else (at, some(vals(ss.lower)))
    }

    def toIterable() : Iterable[(NodeLabel, A)] = truncation.leaves.zip(vals)

    def restrict(ss : Subset) : LeafMap[A] =
      LeafMap(truncation.restrict(ss), vals.slice(ss.lower, ss.upper))

    def size() : Int = vals.size

    def slice(ss : Subset) : Iterator[A] = vals.slice(ss.lower, ss.upper).toIterator

    def mergeSubtreeWithIdx(at : NodeLabel, op : (A, A) => A) : (Option[Int], LeafMap[A]) = {
      val ss = truncation.subtree(at)
      if(ss.size == 0) (none(), this)
      else {
        val oldLeaves : Vector[NodeLabel] = truncation.leaves
        var newLeaves : Array[NodeLabel] = Array.fill(oldLeaves.size - ss.size + 1)(rootLabel)
        oldLeaves.slice(0, ss.lower).copyToArray(newLeaves)
        oldLeaves.slice(ss.upper, oldLeaves.length).copyToArray(newLeaves, ss.lower+1)
        newLeaves(ss.lower) = at

        val newTruncation = Truncation(newLeaves.toVector)

        val oldVals : Vector[A] = vals
        var newVals : Array[A] = Array.fill(oldVals.size - ss.size + 1)(vals(0))
        oldVals.slice(0, ss.lower).copyToArray(newVals)
        oldVals.slice(ss.upper, oldVals.length).copyToArray(newVals, ss.lower+1)
        newVals(ss.lower) = slice(ss).reduce(op)

        (some(ss.lower), LeafMap(newTruncation, newVals.toVector))
      }
    }

    def mergeSubtreeCheckCherry(at : NodeLabel, op : (A, A) => A) : (Option[(NodeLabel, A)], LeafMap[A]) = {
      val (idxOpt, t) = mergeSubtreeWithIdx(at, op)
      idxOpt match {
        case None => (None, t)
        case Some(idx) =>
          t.truncation.cherryAtIdx(idx) match {
            case None => (None, t)
            case Some(vs) => {
              val lab = t.truncation.leaves(vs(0))
              if(lab == rootLabel)
                (none(), t)
              else
                (some((lab.parent, vs.map(t.vals(_)).reduce(op))), t)
            }
          }
      }
    }

    def cherries(op : (A, A) => A) : Iterator[(NodeLabel, A)] =
      truncation.cherries().map(x => (truncation.leaves(x(0)).parent, x.map(vals(_)).reduce(op)))

    def mergeSubtree(at : NodeLabel, op : (A, A) => A) : LeafMap[A] =
      mergeSubtreeWithIdx(at, op)._2

    def leaves() : Vector[NodeLabel] = truncation.leaves

    def toMap() : Map[NodeLabel, A] =
      toIterable().toMap

    // TODO: Figure out if this can be done more efficently
    def internal(base : A, op : (A, A) => A) : Stream[(NodeLabel, A)] = {
      def go(lab : NodeLabel, bound : Subset) : (A, Stream[(NodeLabel, A)]) = {
        val newBound = truncation.subtreeWithin(lab, bound)
        if(newBound.size == 0)
          (base, Stream.empty)
        else if(newBound.size == 1 && truncation.leaves(newBound.lower) == lab)
          (vals(newBound.lower), Stream.empty)
        else {
          val (lacc, lseq) = go(lab.left, newBound)
          val (racc, rseq) = go(lab.right, newBound)
          val acc = op(lacc, racc)
          (acc, (lab, acc) #:: lseq #::: rseq)
        }
      }
      go(rootLabel, truncation.allNodes)._2
    }

    def minimalCompletionNodes() : Stream[(NodeLabel, Option[A])] = {
      // Figure out scalas weird do-notation equivalent
      truncation.minimalCompletionNodes().map {
        case (lab, None) => (lab, none())
        case (lab, Some(i)) => (lab, some(vals(i)))
      }
    }
  }

  def fromNodeLabelMap[A:ClassTag](xs : Map[NodeLabel, A]) : LeafMap[A] = {
    val (labs, vals) = xs.toVector.sortWith({case(x,y) => isStrictLeftOf(x._1, y._1)}).unzip
    LeafMap(Truncation(labs), vals)
  }

  def fringes[A](f : LeafMap[A], t : Truncation) : LeafMap[LeafMap[A]] =
    LeafMap(t, t.leaves.map({ case x => f.restrict(f.truncation.subtree(x)) }))

  // // TODO: WARNING, DOES *NOT* CHECK that the input is coherent (i.e. that the
  // // value below a node is a tree below that node)
  // def joinLeafMap[A:ClassTag](f : LeafMap[LeafMap[A]]) : LeafMap[A] =
  //   LeafMap( Truncation(f.vals.map(_.truncation.leaves).fold(Vector())(_++_)),
  //            f.vals.map(_.vals).fold(Vector())(_++_)    )

  // TODO: Warning does not check that things are ordered coherently!
  def concatLeafMaps[A:ClassTag](f : Vector[LeafMap[A]]) : LeafMap[A] =
    LeafMap( Truncation(f.map(_.truncation.leaves).fold(Vector())(_++_)),
             f.map(_.vals).fold(Vector())(_++_)    )

  ////////

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

  // def spatialTreeRootedAt(rootCell : Rectangle) : SpatialTree = SpatialTree(rootCell)
  def uniformTreeRootedAt(rootCell : Rectangle) : SpatialTree = UniformSplitTree(rootCell)
  def widestSideTreeRootedAt(rootCell : Rectangle) : SpatialTree = WidestSplitTree(rootCell)

  type PartitioningStrategy = RDD[MLVector] => SpatialTree

  val widestSideTreeBounding : PartitioningStrategy = (points => widestSideTreeRootedAt(boundingBox(points)))
  val uniformSideTreeBounding : PartitioningStrategy = (points => uniformTreeRootedAt(boundingBox(points)))

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

  type Probability = Double

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

    def backtrackWithNodes[H](prio : PriorityFunction[H])(implicit ord : Ordering[H]) : Iterator[(H, NodeLabel, Histogram)] = {
      val start = counts.cherries(_+_).map {
        case (lab, c) => (prio(lab, c, tree.volumeAt(lab)), lab)
      }.toSeq

      val thisOuter : Histogram = this

      class HistogramIterator extends Iterator[(H, NodeLabel, Histogram)] {
        val q = PriorityQueue(start: _*)(ord.reverse.on(_._1))
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

      new HistogramIterator()
    }

    def backtrackToWithNodes[H](prio : PriorityFunction[H], hparent : Histogram)(implicit ord : Ordering[H])
        : Iterator[(H, NodeLabel, Histogram)] = {

      class BacktrackToIterator extends Iterator[(H, NodeLabel, Histogram)] {
        // Split the histogram into one histogram per fringe-tree at a leaf in
        // hparent...
        var current = fringes(counts, hparent.truncation)
        val backtracks = current match {
          //... and for each one perform a backtrack
          case LeafMap(t, f) =>
            f.zip(t.leaves).map {
              case (finner, r) =>
                // Backtrack each "fringe-tree" histogram until we hit the
                // corresponding leaf in hparent

                // NOTE: This should really be totalCount (for the priorities to
                // be computed correctly) even though it's not the same as the
                // sum of the individual cells, make sure this is not a problem
                Histogram(tree, totalCount, finner).
                  backtrackWithNodes(prio).
                  takeWhile(_._2 != r.parent)
            }
        }

        val pqInit = backtracks.zipWithIndex.filter(_._1.hasNext).map {
          case (x, i) =>
            val (p1, lab1, h1) = x.next()
            (p1, i, lab1, h1)
        }

        val q : PriorityQueue[(H, Int, NodeLabel, Histogram)] = PriorityQueue(pqInit: _*)(ord.reverse.on(_._1))

        override def hasNext() : Boolean = !q.isEmpty

        override def next() : (H, NodeLabel, Histogram) = {
          val (p, i, lab, h) = q.dequeue()
          if(!backtracks(i).isEmpty) {
            backtracks(i).next() match {
              case (pNew, labNew, hNew) =>
                q += ((pNew, i, labNew, hNew))
            }
          }

          // TODO: Turn this into a pure Vector
          current = current match {
            case LeafMap(t, f) => LeafMap(t, f.updated(i, h.counts))
          }

          (p, lab, Histogram(tree, totalCount, concatLeafMaps(current.vals)))
        }
      }

      new BacktrackToIterator()
    }

    def backtrackNodes[H](prio : PriorityFunction[H])(implicit ord : Ordering[H])
        : Iterator[NodeLabel] =
      backtrackWithNodes(prio)(ord).map(_._2)

    def backtrack[H](prio : PriorityFunction[H])(implicit ord : Ordering[H])
        : Iterator[Histogram] =
      backtrackWithNodes(prio)(ord).map(_._3)

    def backtrackNodes[H](prio : PriorityFunction[H], hparent : Histogram)(implicit ord : Ordering[H]) : Iterator[NodeLabel] =
      backtrackToWithNodes(prio, hparent)(ord).map(_._2)

    def backtrackTo[H](prio : PriorityFunction[H], hparent : Histogram)(implicit ord : Ordering[H]) : Iterator[Histogram] =
      backtrackToWithNodes(prio, hparent)(ord).map(_._3)
  }

  // WARNING: Does not check coherent ordering or same bounding box/tree!
  // def concatHistograms(f : Vector[Histogram]) : Histogram =
  //   Histogram(f.head.tree, f.map(_.totalCount).sum, concatLeafMaps(f.map(_.counts)))

  ////////

  type Count = Long

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

  def partitionPoints(tree : SpatialTree, trunc : Truncation, points : RDD[MLVector]) : Partitioned[MLVector] =
    Partitioned(points.map(x => (trunc.descendUntilLeaf(tree.descendBox(x)).lab, x)))

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

  def histogramFrom(tree : SpatialTree, trunc : Truncation, points : RDD[MLVector], limits : SplitLimits, stop : StopRule) : Histogram = {
    val counts = splitAndCountFrom(tree, trunc, points, limits, stop)
    val totalCount = counts.values.sum
    Histogram(tree, totalCount, fromNodeLabelMap(counts))
  }

  def histogramStartingWith(h : Histogram, points : RDD[MLVector], limits : SplitLimits, stop : StopRule) : Histogram =
    histogramFrom(h.tree, h.truncation, points, limits, stop)

  def histogram(points : RDD[MLVector],
                limits : SplitLimits,
                stop : StopRule = noEarlyStop,
                partitioningStrategy : PartitioningStrategy = widestSideTreeBounding) : Histogram =
    histogramFrom(partitioningStrategy(points), rootTruncation, points, limits, stop)
}
