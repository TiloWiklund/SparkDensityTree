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
import org.apache.spark.mllib.random.RandomRDDs.normalVectorRDD

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{ Vector => MLVector, _ }

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{ Logger, Level }

// import scala.util.Sorting

object ScalaDensity {

  // Axis parallel (bounding) boxes

  type Axis = Int
  type Intercept = Double
  type Volume = Double

  case class Rectangle(low : Vector[Double], high : Vector[Double]) {
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

    def volume() : Volume =
      ((high, low).zipped map (_-_)) reduce (_*_)

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

  case class NodeLabel(lab : BigInt) {
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

    // Auxiliary stuff
    def invert() : NodeLabel =
      NodeLabel(lab ^ ((1 << (lab.bitLength-1)) - 1))
    def initialLefts() : Int =
      lab.bitLength - lab.clearBit(lab.bitLength-1).bitLength - 1
    def initialRights() : Int =
      invert().initialLefts

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
                                 right : (NodeLabel, A) => A) {
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
  case class Subset(lower : Int, upper : Int) {
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

  case class Truncation(leaves : Vector[NodeLabel]) {
    // TODO: Make this a more efficent binary search
    def subtreeWithin(at : NodeLabel, within : Subset) : Subset = {
      // val mid = (within.lower until within.upper)
      //   .dropWhile(i => isStrictLeftOf(leaves(i), at))
      //   .takeWhile(i => at == leaves(i) || isDescendantOf(leaves(i), at))

      // if(mid.length > 0)
      //   Subset(mid.head, mid.last+1)
      // else
      //   Subset(0, 0)

      // if(within.isEmpty || isStrictRightOf(at, leaves(within.upper-1)) || isStrictLeftOf(at, leaves(within.lower))) {
      //      Subset(0, 0)
      // } else {
      val low = binarySearchWithin((x : NodeLabel) => !isStrictLeftOf(x, at))(leaves, within)
      val high = binarySearchWithin((x : NodeLabel) => x != at && !isDescendantOf(x, at))(leaves, within.sliceLow(low))
      Subset(low, high).normalise
      // }
    }

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
      val inner = (0 until leaves.length).sliding(2).flatMap {
        case x =>
          val l = leaves(x(0))
          val r = leaves(x(1))
          if(l.sibling == r) { List(Vector(x(0), x(1))) }
          else {
            val lonerl =
              if(l.isLeft && !inRightSubtreeOf(r, l.parent)) List(Vector(x(0)))
              else                                           List()
            val lonerr =
              if(r.isRight && !inLeftSubtreeOf(l, r.parent)) List(Vector(x(1)))
              else                                           List()
            lonerl ++ lonerr
          }
      }.toIterator

      val leftcorner  = (if(leaves.head.isRight) List(Vector(0)) else List()).toIterator
      val rightcorner = (if(leaves.last.isLeft) List(Vector(leaves.length-1)) else List()).toIterator

      leftcorner ++ inner ++ rightcorner
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
      if(leaves.length == 0)
        Stream((rootLabel, none()))
      else {
        // If necessary add a left/right-most leaf
        val firstLeaf = leaves.head
        val lastLeaf = leaves.last
        val llim = firstLeaf.truncate(firstLeaf.initialLefts)
        val rlim = lastLeaf.truncate(lastLeaf.initialRights)
        val l : Stream[(NodeLabel, Option[Int])] = if(llim != firstLeaf) Stream((llim, none())) else Stream.empty
        val r : Stream[(NodeLabel, Option[Int])] = if(rlim != lastLeaf) Stream((rlim, none())) else Stream.empty
        val c : Stream[(NodeLabel, Option[Int])] = leaves.toStream.zip(0 until leaves.length).map(x => (x._1, some(x._2)))

        val leavesWidened : Stream[(NodeLabel, Option[Int])] = l #::: c #::: r

        val leavesFilled : Stream[(NodeLabel, Option[Int])] = leavesWidened.sliding(2).flatMap {
          case ((llab, _) #:: (rlab, rval) #:: Stream.Empty) =>
            val j = join(llab, rlab)
            val upPath   = path(llab, j.left).filter(_.isLeft).map(x => (x.sibling, none()))
            val downPath = path(j.right, rlab).filter(_.isRight).map(x => (x.sibling, none()))
            upPath #::: downPath #::: Stream((rlab, rval))
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


  // TODO: Figure out something more efficient
  // def cutVector[A:ClassTag](xs : Vector[A], from : Int, to : Int) : Vector[A] =
  //   (xs.slice(0, from) ++ xs.slice(to, xs.length)).toVector

  case class LeafMap[A:ClassTag](truncation : Truncation, vals : Vector[A]) {
    // TODO: Optimise?
    def query(labs : Walk) : (NodeLabel, Option[A]) = {
      val (at, ss) = truncation.descendUntilLeafWhere(labs)
      if(ss.isEmpty) (at, none()) else (at, some(vals(ss.lower)))
    }

    def toIterable() : Iterable[(NodeLabel, A)] = truncation.leaves.zip(vals)

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
        // val newLeaves = truncation.leaves.slice(0, ss.lower).toSeq ++ Seq(at) ++ truncation.leaves.slice(ss.upper, truncation.leaves.length).toSeq
        // val newVals = vals.slice(0, ss.lower).toSeq ++ Seq(slice(ss).reduce(op)) ++ vals.slice(ss.upper, vals.length).toSeq
        // LeafMap(Truncation(newLeaves.toVector), newVals.toVector)

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

  ////////

  // TODO: Can we figure out some clever way to do memoisation/caching?
  case class SpatialTree(rootCell : Rectangle) {
    def dimension() : Int = rootCell.dimension

    def volumeTotal() : Double = rootCell.volume

    def volumeAt(at : NodeLabel) : Double =
      rootCell.volume / pow(2, at.depth)

    def axisAt(at : NodeLabel) : Int =
      at.depth % dimension()

    def cellAt(at : NodeLabel) : Rectangle =
      unfoldTree(rootCell)((lab, box) => box.lower(axisAt(lab.parent)),
                           (lab, box) => box.upper(axisAt(lab.parent)))(at)

    def cellAtCached() : CachedUnfoldTree[Rectangle] =
      unfoldTreeCached(rootCell)((lab, box) => box.lower(axisAt(lab.parent)),
                                 (lab, box) => box.upper(axisAt(lab.parent)))

    def descendBoxPrime(point : MLVector) : Stream[(NodeLabel, Rectangle)] = {
      def step(lab : NodeLabel, box : Rectangle) : (NodeLabel, Rectangle) = {
        val along = axisAt(lab)

        if(box.isStrictLeftOfCentre(along, point))
          (lab.left, box.lower(along))
        else
          (lab.right, box.upper(along))
      }

      Stream.iterate((rootLabel, rootCell))(Function.tupled(step))
    }

    def descendBox(point : MLVector) : Stream[NodeLabel] = descendBoxPrime(point).map(_._1)
  }

  def spatialTreeRootedAt(rootCell : Rectangle) : SpatialTree = SpatialTree(rootCell)

  // WARNING: Approx. because it does not merge cells where removing one point
  // puts it below the splitting criterion
  def looL2ErrorApproxFromCells(total : Count, cells : Iterable[(Volume, Count)]) : Double =
    cells.map {
      case (v : Volume, c : Count) =>
        // (c/(v*total))^2 * v
      	val dtotsq = (c/total)*(c/total)/v
        // 1/total * (c-1)/(v*(total-1)) * c
        val douts = c*(c-1)/(v*(total - 1)*total)
        dtotsq - 2*douts
    }.sum

  type PriorityFunction[H] = (NodeLabel, Count, Volume) => H

  // class PriorityIterator[S,A](init : S, inits : Seq[A])(pull : A => Option[A])(implicit ord : Ordering[A]) extends Iterator[A] {
  //   val pq = PriorityQueue(init: _*)(ord)
  //   val s = 
  //   override def hasNext : Boolean = !pq.isEmpty
  //   override def next() : A = {
  //     val a = pq.enqueue()
  //     pull(a) match {
  //       None => ()
  //       Some(anew) => pq += a
  //     }
  //     a
  //   }
  // }

  type Probability = Double

  case class TailProbabilities(tree : SpatialTree, tails : LeafMap[Probability]) {
    def query(v : MLVector) : Double = {
      tails.query(tree.descendBox(v)) match {
        case (_, None)    => 0
        case (_, Some(p)) => p
      }
    }
  }

  case class Histogram(tree : SpatialTree, totalCount : Count, counts : LeafMap[Count]) {
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

    def backtrackWithNodes[H](prio : PriorityFunction[H])(implicit ord : Ordering[H]) : Iterator[(NodeLabel, Histogram)] = {
      val start = counts.cherries(_+_).map {
        case (lab, c) => (prio(lab, c, tree.volumeAt(lab)), lab)
      }.toSeq

      val thisOuter : Histogram = this

      class HistogramIterator extends Iterator[(NodeLabel, Histogram)] {
        val q = PriorityQueue(start: _*)(ord.reverse.on(_._1))
        var h = thisOuter
        override def hasNext : Boolean = !q.isEmpty
        override def next() : (NodeLabel, Histogram) = {
          val (_, lab : NodeLabel) = q.dequeue()

          // println(h.counts.vals.size)
          // if(!h.counts.truncation.hasAsCherry(lab)) {
          //   println("GOT NON-CHERRY!")
          //   println(lab)
          //   println(h.counts.truncation.viewSubtree(lab,1))
          // } else {
          //   print("OK: ")
          //   println(lab)
          // }

          val (cOpt, countsNew) = h.counts.mergeSubtreeCheckCherry(lab, _+_)

          val hnew = Histogram(h.tree, h.totalCount, countsNew)

          cOpt match {
            case None => ()
            case Some((cLab, cCnt)) => {
              q += ((prio(cLab, cCnt, tree.volumeAt(cLab)), cLab))
              // print("Adding: ")
              // println(cLab)
            }
          }

          h = hnew

          (lab, hnew)
        }
      }

      new HistogramIterator()

      // val q = PriorityQueue[(H, NodeLabel)](start: _*)(ord.reverse.on(_._1))
      // println(q)
      // Stream.iterate((rootLabel, this)) {
      //   case (_, h : Histogram) =>
      //     //Ugly HACK part 1!
      //     if(q.isEmpty) (NodeLabel(0), h)
      //     else {
      //       val (_, lab) = q.dequeue()

      //       // println(q)

      //       val (cOpt, countsNew) = h.counts.mergeSubtreeCheckCherry(lab, _+_)

      //       println(lab)
      //       println(cOpt)
      //       // println(countsNew)

      //       val hnew = Histogram(h.tree, h.totalCount, countsNew)

      //       cOpt match {
      //         case None => ()
      //         case Some((cLab, cCnt)) => q += ((prio(cLab, cCnt, tree.volumeAt(cLab)), cLab))
      //       }

      //       (lab, hnew)
      //     }
      // }.tail.takeWhile(_._1 != NodeLabel(0)) //Ugly HACK part 2
    }

    def backtrackNodes[H](prio : PriorityFunction[H])(implicit ord : Ordering[H])
        : Iterator[NodeLabel] =
      backtrackWithNodes(prio)(ord).map(_._1)

    def backtrack[H](prio : PriorityFunction[H])(implicit ord : Ordering[H])
        : Iterator[Histogram] =
      backtrackWithNodes(prio)(ord).map(_._2)

    //   backtrackNodes(prio).scanLeft(this) {
    //     // NOTE: This results in two extra lookups, but is nicer API-wise
    //     case (h : Histogram, (lab, _)) =>
    //       // println(lab)
    //       val newCounts = h.counts.mergeSubtree(lab, _+_)
    //       // assert(!counts.truncation.leaves.contains(lab))
    //       assert(counts.truncation.subtree(lab).size > 0)
    //       // assert(counts.truncation.leaves(counts.truncation.subtree(lab).lower) != lab)
    //       if(!newCounts.truncation.leaves.contains(lab)) {
    //         // TODO: Debug
    //         // print("Lv1: ")
    //         // println(counts.truncation.leaves.toList)
    //         // print("Lv2: ")
    //         // println(newCounts.truncation.leaves.toList)
    //         // print("Lab: ")
    //         // println(lab)
    //         // println("...")
    //         // print("SS1: ")
    //         // println(counts.truncation.slice(counts.truncation.subtree(lab)).toList)
    //         // print("SS2: ")
    //         // println(newCounts.truncation.slice(counts.truncation.subtree(lab)).toList)
    //         // assert(newCounts.truncation.leaves.contains(lab))
    //       }
    //       Histogram(tree, totalCount, newCounts)
    //   }.tail
    // }
  }

  ////////

  type Count = Long

  // TODO: This should maybe be parameterised over Count/countByKey as well
  case class Partitioned[A](points : RDD[(BigInt, A)]) {
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

  def histogram(points : RDD[MLVector], limits : SplitLimits, stop : StopRule) : Histogram =
    histogramFrom(spatialTreeRootedAt(boundingBox(points)),
                  rootTruncation, points, limits, stop)

  def main(args: Vector[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("ScalaDensity").setMaster("local[3]")//setMaster("spark://127.0.0.1:7077")
    val sc = new SparkContext(conf)

    val dfnum = 2000
    val dfdim = 3
    // val df = normalVectorRDD(sc, n, 2)
    val df = normalVectorRDD(sc, dfnum, dfdim, 3, 7387389)

    def supportCarveLim(totalVolume : Double, totalCount : Count)(depth : Int, volume : Volume, count : Count) =
      count > dfnum/2 || (1 - count/totalCount)*volume/totalVolume > 0.01

    def countLim(totalVolume : Double, totalCount : Count)(depth : Int, volume : Volume, count : Count) =
      count > 10

    // factor out totalCount and totalVolume since they are the same for all nodes
    def supportCarvePriority(lab : NodeLabel, c : Count, v : Volume) : Double = (1 - c)*v
    def countPriority(lab : NodeLabel, c : Count, v : Volume) : Count = c

    val supportCarvedH = histogram(df, supportCarveLim, noEarlyStop)

    val temp = 0.005

    var maxH = supportCarvedH
    var maxLik = supportCarvedH.logPenalisedLik(temp)

    supportCarvedH.backtrack(supportCarvePriority).foreach {
      case h =>
        val hLik = h.logPenalisedLik(1/temp)
        if(hLik > maxLik) {
          maxH = h
          maxLik = hLik
        }
    }

    val tributaryH = histogramStartingWith(maxH, df, countLim, noEarlyStop)

    var minH = tributaryH
    var minLoo = minH.looL2ErrorApprox
    tributaryH.backtrack(countPriority).foreach {
      case h =>
        val hLoo = h.looL2ErrorApprox
        if(hLoo < minLoo) {
          minH = h
          minLoo = hLoo
        }
    }

    println("LOO scores:")
    print("Support carved end: ")
    println(supportCarvedH.looL2ErrorApprox)
    print("Support carved maxLik: ")
    println(maxH.looL2ErrorApprox)
    print("Tributary end: ")
    println(tributaryH.looL2ErrorApprox)
    print("Tributary minLoo: ")
    println(minH.looL2ErrorApprox)

    // // Print the root box
    // println("Root box")
    // val rootbox = h.tree.rootCell.factorise
    // rootbox.foreach {
    //   case (l, u) => println(List("[",l,",",u,"]").mkString)
    // }

    // println("Total count")
    // println(h.totalCount)

    // // Print depths
    // println("Depths")
    // val depths = h.counts.minimalCompletionNodes.map {
    //   case (lab, _) => lab.depth
    // }
    // depths.foreach(println(_))

    // // Print counts
    // println("Counts")
    // val counts = h.counts.minimalCompletionNodes.map {
    //   case (_, None) => 0
    //   case (_, Some(c)) => c
    // }
    // counts.foreach(println(_))

    // // Print Volume
    // println("Volume")
    // val volumes = h.counts.minimalCompletionNodes.map {
    //   case (lab, _) => h.tree.volumeAt(lab)
    // }
    // volumes.foreach(println(_))

    // // Print Probability
    // println("Probability")
    // val probabilities = h.counts.minimalCompletionNodes.map {
    //   case (_, None)    => 0
    //   case (_, Some(c)) => c.toDouble/h.totalCount
    // }
    // probabilities.foreach(println(_))

    // // Print Density
    // println("Density")
    // val densities = h.counts.minimalCompletionNodes.map {
    //   case (_, None)      => 0
    //   case (lab, Some(c)) => c/(h.tree.volumeAt(lab) * h.totalCount)
    // }
    // densities.foreach(println(_))

    sc.stop()
  }

  def testRun() : Histogram = {
    val dfnum = 5000
    val dfdim = 3
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("ScalaDensityTest").setMaster("local")
    val sc = new SparkContext(conf)
    val df = normalVectorRDD(sc, dfnum, dfdim, 6, 7387389).cache()
    def lims(totalVolume : Double, totalCount : Count)(depth : Int, volume : Volume, count : Count) =
      count > dfnum/2 || (1 - count/totalCount)*volume > 0.001*totalVolume
    histogram(df, lims, noEarlyStop)
  }
}
