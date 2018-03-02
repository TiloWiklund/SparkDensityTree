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

  case class Rectangle(low : Array[Double], high : Array[Double]) {
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
      (high, low, v.toArray).zipped.forall { case (h, l, c) => h >= c && c >= l }

    def isLeftOfCentre(along : Axis, v : MLVector) : Boolean =
      v(along) <= centre(along)

    def isRightOfCentre(along : Axis, v : MLVector) : Boolean =
      v(along) >  centre(along)
  }

  def hull(b1 : Rectangle, b2 : Rectangle) : Rectangle =
    Rectangle( ( b1.low, b2.low ).zipped map min,
               (b1.high, b2.high).zipped map max )

  def point(v : MLVector) : Rectangle =
    Rectangle(v.toArray, v.toArray)

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

  object leftRightOrd extends Ordering[NodeLabel] {
    def compare(a : NodeLabel, b : NodeLabel) = a.truncate(b.depth).lab compare b.truncate(a.depth).lab
  }

  def isAncestorOf(a : NodeLabel, b : NodeLabel) : Boolean =
    a.lab < b.lab && leftRightOrd.compare(a, b) == 0

  def isDescendantOf(a : NodeLabel, b : NodeLabel) : Boolean =
    isAncestorOf(b, a)

  def isLeftOf(a : NodeLabel, b : NodeLabel) : Boolean =
    leftRightOrd.compare(a, b) < 0

  def isRightOf(a : NodeLabel, b : NodeLabel) : Boolean =
    isLeftOf(b, a)

  def inLeftSubtreeOf(a : NodeLabel, b : NodeLabel) : Boolean =
    a == b.left || isAncestorOf(b.left, a)

  def inRightSubtreeOf(a : NodeLabel, b : NodeLabel) : Boolean =
    a == b.right || isAncestorOf(b.right, a)

  def adjacent(a : NodeLabel, b : NodeLabel) : Boolean =
    a.parent == b || b.parent == a

  def join(a : NodeLabel, b : NodeLabel) : NodeLabel = {
    val d = min(a.depth, b.depth)
    val aT = a.truncate(d)
    val bT = b.truncate(d)
    aT.ancestor((aT.lab ^ bT.lab).bitLength)
  }

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
  }

  type Walk = Stream[NodeLabel]

  case class Truncation(leaves : Array[NodeLabel]) {
    // TODO: Make this a more efficent binary search
    def subtreeWithin(at : NodeLabel, within : Subset) : Subset = {
      val mid = (within.lower until within.upper)
        .dropWhile(i => isLeftOf(leaves(i), at))
        .takeWhile(i => at == leaves(i) || isDescendantOf(leaves(i), at))

      if(mid.length > 0)
        Subset(mid.head, mid.last+1)
      else
        Subset(0, 0)
    }

    def allNodes() : Subset = Subset(0, leaves.length)

    def subtree(at : NodeLabel) : Subset = subtreeWithin(at, allNodes())

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
      Truncation(minimalCompletionNodes.map(_._1).toArray)
  }

  // WARNING: Currently does not check whether leaves can be a set of leaves
  // (i.e. that it does not contain ancestor-descendant pairs)
  def fromLeafSet(leaves : Iterable[NodeLabel]) : Truncation =
    Truncation(leaves.toArray.sorted(leftRightOrd))

  def rootTruncation() : Truncation = Truncation(Array(rootLabel))

  def some[A](a : A) : Option[A] = Some(a)
  def none[A]() : Option[A] = None

  // TODO: Figure out something more efficient
  // def cutArray[A:ClassTag](xs : Array[A], from : Int, to : Int) : Array[A] =
  //   (xs.slice(0, from) ++ xs.slice(to, xs.length)).toArray

  case class LeafMap[A:ClassTag](truncation : Truncation, vals : Array[A]) {
    // TODO: Optimise?
    def query(labs : Walk) : (NodeLabel, Option[A]) = {
      val (at, ss) = truncation.descendUntilLeafWhere(labs)
      if(ss.isEmpty) (at, none()) else (at, some(vals(ss.lower)))
    }

    def toIterable() : Iterable[(NodeLabel, A)] = truncation.leaves.zip(vals)

    def size() : Int = vals.size

    def slice(ss : Subset) : Iterator[A] = vals.slice(ss.lower, ss.upper).toIterator

    def mergeSubtree(at : NodeLabel, op : (A, A) => A) : LeafMap[A] = {
      val ss = truncation.subtree(at)
      if(ss.size == 0) this
      else {
        // println(ss.size)
        val newLeaves = truncation.leaves.slice(0, ss.lower) ++ Seq(at) ++ truncation.leaves.slice(ss.upper, truncation.leaves.length)
        // println(truncation.leaves.toList)
        // println(newLeaves.toList)
        val newVals = vals.slice(0, ss.lower) ++ Seq(slice(ss).reduce(op)) ++ vals.slice(ss.upper, vals.length)
        // println(vals.toList)
        // println(newVals.toList)
        LeafMap(Truncation(newLeaves.toArray), newVals.toArray)
      }
    }

    def leaves() : Array[NodeLabel] = truncation.leaves

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
    val (labs, vals) = xs.toArray.sortWith({case(x,y) => isLeftOf(x._1, y._1)}).unzip
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

        if(box.isLeftOfCentre(along, point))
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

  case class Histogram(tree : SpatialTree, totalCount : Count, counts : LeafMap[Count]) {
    def density(v : MLVector) : Double = {
      counts.query(tree.descendBox(v)) match {
        case (_, None) => 0
        case (at, Some(c)) =>
          c / (totalCount * tree.volumeAt(at))
      }
    }

    def truncation() : Truncation = counts.truncation

    def cells() : Iterable[(Volume, Count)] = counts.toIterable.map {
      case (lab : NodeLabel, c : Count) => (tree.volumeAt(lab), c)
    }

    def looL2ErrorApprox() : Double = looL2ErrorApproxFromCells(totalCount, cells())

    def logLik() : Double = counts.toIterable.map {
        case (lab : NodeLabel, c : Count) => c*log(c/(totalCount * tree.volumeAt(lab)))
    }.sum

    def logPenalisedLik(taurec : Double) : Double =
      log(exp(taurec) - 1) - counts.toIterable.size*taurec + logLik()

    def backtrackNodes[H](prio : PriorityFunction[H])(implicit ord : Ordering[H])
        : Stream[(NodeLabel, Count)] = {

      // TODO: Rewrite this in terms of PartialOrder[NodeLabel] and lexicographic order
      object BacktrackOrder extends Ordering[(H, NodeLabel, Count)] {
        def compare(x : (H, NodeLabel, Count), y : (H, NodeLabel, Count)) = {
          val (xH, xLab, _) = x
          val (yH, yLab, _) = y
          if(isAncestorOf(xLab, yLab)) 1
          else if(isAncestorOf(yLab, xLab)) -1
          else ord.compare(xH, yH)
        }
      }

      var q = new PriorityQueue()(BacktrackOrder.reverse)
      for((lab, cacc) <- counts.internal(0, _+_)) {
        q += ((prio(lab, cacc, tree.volumeAt(lab)), lab, cacc))
      }

      q.dequeueAll.toStream.map({case (_, lab, c) => (lab, c)})
    }

    def backtrack[H](prio : PriorityFunction[H])(implicit ord : Ordering[H]) : Stream[Histogram] = {
      backtrackNodes(prio).scanLeft(this) {
        // NOTE: This results in two extra lookups, but is nicer API-wise
        case (h : Histogram, (lab, _)) =>
            Histogram(tree, totalCount, h.counts.mergeSubtree(lab, _+_))
      }.tail
    }
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

  def main(args: Array[String]) = {
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
}
