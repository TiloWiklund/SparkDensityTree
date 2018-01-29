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
import scalaz.{ Ordering => OrderingZ, _ }
import Scalaz._
// import co.theasi.plotly.{Plot, draw, ScatterOptions, ScatterMode}
// import scalax.chart.api._
import vegas._
// import vegas.render.WindowRenderer._

import scala.math.{min, max, log, exp}
import scala.math.BigInt._

import scala.collection.mutable.{ HashMap, PriorityQueue }
import scala.collection.mutable.{ Set => MSet, Map => MMap }
import scala.collection.{mutable, immutable}
import scala.collection.immutable.{Set, Map}

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{ Vector => MLVector, _ }
import org.apache.spark.mllib.random.RandomRDDs.normalVectorRDD

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{ Logger, Level }

import java.io.IOException
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

import scala.util.Sorting

object ScalaDensity {
  case class Rectangle(low : Array[Double], high : Array[Double]) {
    override def toString = (low, high).zipped.toArray.mkString("x")

    def dim() = high.length

    def centre(along : Int) : Double =
      (high(along) + low(along))/2

    def split(along : Int) : (Rectangle, Rectangle) = split(along, centre(along))

    def split(along : Int, thresh : Double) : (Rectangle, Rectangle) = {
      val c = min(max(thresh, low(along)), high(along))
      (Rectangle(low,                   high.updated(along, c)),
       Rectangle(low.updated(along, c), high                  ))
    }

    def lower(along : Int) : Rectangle = split(along)._1
    def lower(along : Int, thresh : Double) : Rectangle = split(along, thresh)._1

    def upper(along : Int) : Rectangle = split(along)._2
    def upper(along : Int, thresh : Double) : Rectangle = split(along, thresh)._2

    def volume() : Double =
      ((high, low).zipped map (_-_)) reduce (_*_)
  }

  def point(v : MLVector) : Rectangle = Rectangle(v.toArray, v.toArray)

  def hull(b1 : Rectangle, b2 : Rectangle) : Rectangle =
    Rectangle( ( b1.low, b2.low ).zipped map min,
               (b1.high, b2.high).zipped map max )

  type NodeLabel = BigInt
  val rootLabel : NodeLabel = 1

  def  left(x : NodeLabel) : NodeLabel = 2*x
  def right(x : NodeLabel) : NodeLabel = 2*x + 1

  def isAncestorOf(a : NodeLabel, b : NodeLabel) : Boolean =
    a < b && ((b >> (b.bitLength - a.bitLength)) == a)

  type InfiniteTree[T] = NodeLabel => T
  type FiniteTree[T] = Map[NodeLabel, T]

  def binarySplitTree(bbox : Rectangle) : InfiniteTree[(Rectangle, Double, Int)] = {
    val d = bbox.dim

    // Tree that caches bounding box and depth of every node
    lazy val t : InfiniteTree[(Rectangle, Double, Int)] =
      Memo.mutableHashMapMemo {
        case `rootLabel` => (bbox, bbox.centre(0), 0)
        case n =>
          val (pbox, c, paxis) = t(n >> 1)
          val box =
            if(n.testBit(0))
              pbox.upper(paxis, c)
            else
              pbox.lower(paxis, c)
          val axis = (paxis + 1) % d
          (box, box.centre(axis), axis)
      }

    return t
  }

  case class Histogram( boxes  : FiniteTree[Rectangle],
                        splits : FiniteTree[(Double, Int)],
                        counts : FiniteTree[Long],
                        leaves : Set[NodeLabel] )

  // Produce a stream of (Histogram, new nodes) pairs given
  // stream of As (usually A is the type of some stopping threshold )
  case class HistogramStream[A](next : A => (Histogram, Set[NodeLabel], HistogramStream[A])) {
    // Iterate until fixed point
    def fix(v : A) : Stream[Histogram] = {
      val (h, n, hs) = next(v)
      if(n.isEmpty)
        h #:: Stream.empty
      else
        h #:: hs.fix(v)
    }
  }

  def histogramPartial[A]( points : RDD[MLVector],
                           stoprule : (A, Long, Rectangle) => Boolean)
      : HistogramStream[A] = {
    val fullTree = binarySplitTree(points.map(point(_)).reduce(hull))

    val boxTree = fullTree map { case (box, _, _) => box }
    val splitsTree = fullTree map { case (_, thresh, axis) => (thresh, axis) }

    // print("Sanity: ")
    // println(boxTree(rootLabel).volume)
    // println(boxTree(left(rootLabel)).volume + boxTree(right(rootLabel)).volume)
    // println(boxTree(left(left(rootLabel))).volume +
    //           boxTree(left(right(rootLabel))).volume +
    //           boxTree(right(left(rootLabel))).volume +
    //           boxTree(right(right(rootLabel))).volume)
    // println("****")

    def go(  cells : RDD[(NodeLabel, MLVector)], // Current partition cells
         accCounts : Map[NodeLabel, Long], // Current cell counts
       accInternal : Set[NodeLabel], // Current internal nodes
         accLeaves : Set[NodeLabel], // Current leaf nodes
          accBoxes : Map[NodeLabel, Rectangle], // Current cell boundaries
         accSplits : Map[NodeLabel, (Double, Int)] ) // Current splitting hyperpls.
        : HistogramStream[A] =
      HistogramStream {
        // TODO: Some of the accumulator variables can be made mutable
        v : A =>
        val currCounts = cells.countByKey()
        val (doSplit, dontSplit) = currCounts.partition {
          case (lab, count) =>
            stoprule(v, count, accBoxes(lab))
        }
        // TODO: keep empty leaves separate to speed things up a bit
        val emptyLeaves =
          accLeaves.filter(!currCounts.isDefinedAt(_)).map((_, 0L))
        val newCounts = accCounts ++ currCounts ++ emptyLeaves
        val hist = Histogram(accBoxes, accSplits, newCounts, accLeaves)
        if(doSplit.isEmpty) {
          ( hist, Set(),
            go(cells, newCounts, accInternal, accLeaves, accBoxes, accSplits) )
        } else {
          // All leaves that fit the splitting criteria become internal nodes
          val oldLeaves   = doSplit.keySet
          val newInternal = accInternal ++ oldLeaves
          // Both children of all the split leaves become new leaves
          val addLeaves   = oldLeaves.flatMap(x => Array(left(x), right(x))).toSet
          val newLeaves   = (accLeaves -- oldLeaves) ++ addLeaves
          // Pull bounding box and splitting hyperplane from the memoised trees
          val addBoxes    = addLeaves map { x => (x, boxTree(x)) }
          val newBoxes    = accBoxes ++ addBoxes
          //
          val addSplits   =
            (oldLeaves map { x : NodeLabel => (x, splitsTree(x)) }).toMap
          val newSplits   = accSplits ++ addSplits
          // Local checkpoint here for performance reasons
          val newCells = cells.map {
            case (k : NodeLabel, v : MLVector) =>
              if(addSplits.isDefinedAt(k)) {
                val (thresh, axis) = addSplits(k)
                if(v(axis) < thresh) {
                  (left(k), v)
                } else {
                  (right(k), v)
                }
              } else {
                (k, v)
              }
          }.localCheckpoint()
          ( hist, addLeaves,
            go(newCells, newCounts, newInternal, newLeaves, newBoxes, newSplits) )
        }
      }

    return go( points.map((1,_)), // Partition cells
               Map(), // Counts
               Set(), // Internal nodes
               Set(1), // Leaf nodes
               Map( BigInt(1) -> boxTree(1) ), // Cell boundarie
               Map() ) // Splitting hyperplanes
  }

  // Compute support carved histogram (without intermediate stops)
  def supportCarved( points : RDD[MLVector],
                splitThresh : Double,
                     minVol : Double ) : Histogram = {
    val totalCount = points.count()
    def stoprule(noinput : Unit, count : Long, box : Rectangle) : Boolean = {
      val v = box.volume
      val p = (1 - count/totalCount)
      // TODO: Ask Raaz about this criterion again
      ((count == totalCount) | (p * v >= splitThresh)) && (v > minVol)
    }
    histogramPartial(points, stoprule).fix(Unit).last
  }

  // Compute histogram given volume and count bounds
  def histogram( points : RDD[MLVector],
            splitThresh : Long,
                 minVol : Double ) : Histogram = {
    def stoprule(noinput : Unit, count : Long, box : Rectangle) : Boolean =
      (count >= splitThresh) && (box.volume > minVol)
    val hs = histogramPartial(points, stoprule).fix(Unit)
    // for(h <- hs) {
    //   println(probAtNode(h, rootLabel))
    //   println(h.leaves.toList.map(probAtNode(h, _)).sum)
    //   println("++++")
    //   print("Vol: ")
    //   println(h.leaves.toList.map(l => h.boxes(l).volume).sum)
    //   // println("****")
    //   // println(h.leaves.toList.map(l => (l, h.boxes(l).volume)).toMap)
    //   println("++++")
    //   print("Count: ")
    //   println(h.leaves.toList.map(l => h.counts(l)).sum)
    //   // println("****")
    //   // println(h.leaves.toList.map(l => (l, h.counts(l))).toMap)
    //   println("----")
    // }
    hs.last
  }

  def query(f : Histogram, p : MLVector) : NodeLabel = {
    var node = BigInt(1)
    val splits = f.splits
    while(splits.isDefinedAt(node)) {
      val (t, d) = splits(node)
      if(p(d) < t) {
        node = left(node)
      } else {
        node = right(node)
      }
    }
    node
  }

  def density(f : Histogram, p : MLVector) : Double = {
    val node = query(f, p)
    f.counts(node)/(f.boxes(node).volume * f.counts(1))
  }

  // NOTE: This is the quantile thing
  case class Quantiles( splits : FiniteTree[(Double, Int)],
                        probs  : FiniteTree[Double] )

  def densityAtNode(h : Histogram, lab : NodeLabel) : Double = {
    h.counts(lab)/(h.boxes(lab).volume * h.counts(1))
  }

  def probAtNode(h : Histogram, lab : NodeLabel) : Double = {
    h.counts(lab)/(h.counts(1) * 1.0)
  }

  def toQuantiles(h : Histogram) : Quantiles = {
    val splits = h.splits
    val n = h.counts(rootLabel)
    val leafHeights  = h.leaves.toList.map {
      case lab =>
        (lab, densityAtNode(h, lab), h.counts(lab))
    }

    val cumulHeights = leafHeights.
      sortWith(_._2 < _._2).
      scanLeft((rootLabel, 0L))(
        { case ((_, cumc), (lab, _, c)) => (lab, cumc + c) }).
      map( { case (lab, cumc) => (lab, cumc/(n*1.0)) } ).
      tail.toMap

    Quantiles(splits, cumulHeights)
  }

  def queryQuantile(q : Quantiles, p : MLVector) : Double = {
    var node = BigInt(1)
    val splits = q.splits
    while(splits.isDefinedAt(node)) {
      val (t, d) = splits(node)
      if(p(d) < t) {
        node = left(node)
      } else {
        node = right(node)
      }
    }
    q.probs(node)
  }

  // NOTE: This is only approximate since we do not collapse cherries
  //       that no longer satisfy the splitting criterion after removing
  //       the point.
  def looL2ErrorApprox(f : Histogram) : Double = {
    val norm = f.counts(1)
    f.leaves.toList.map{
        x : NodeLabel =>
        val c = f.counts(x)
        val v = f.boxes(x).volume
    	    val dtotsq = (c/norm)*(c/norm)/v // (c/(v*norm))^2 * v
        val douts = c*(c-1)/(v*(norm - 1)*norm) // 1/norm * (c-1)/(v*(norm-1)) * c
        dtotsq - 2*douts
    }.sum
  }

  // def queryDepth(f : InfiniteTree[(Rectangle, Double, Int)], d : Int, p : MLVector) : NodeLabel = {
  //   var node = 1
  //   for(i <- 2 to d) {
  //     val (_, t, d) = f(node)
  //     if(p(d) <= t) {
  //       node = left(node)
  //     } else {
  //       node = right(node)
  //     }
  //   }
  //   node
  // }

  def isRight(x : NodeLabel) : Boolean =  x.testBit(0)
  def  isLeft(x : NodeLabel) : Boolean = !x.testBit(0)
  def parent(x : NodeLabel) : NodeLabel = x >> 1

  def depthFirst[A](t : FiniteTree[A]) : Stream[(Int, NodeLabel, A)] = {
    def go(lev : Int, lab : NodeLabel) : Stream[(Int, NodeLabel, A)] = {
      if(t.isDefinedAt(lab))
        (lev, lab, t(lab)) #:: go(lev+1, left(lab)) #::: go(lev+1, right(lab))
      else
        Stream.empty
    }
    go(1, 1)
  }

  // NOTE: Warning, does not check to make sure something is a cherry!
  def cutCherry(h : Histogram, lab : BigInt) : Histogram = {
    // TODO: Should the others be pruned as well, to conserve memory?
    // print("Cutting ")
    // println(lab)
    val result =
      Histogram( h.boxes, h.splits - lab, h.counts,
                ((h.leaves + lab) - left(lab)) - right(lab) )
    // if(!h.splits.isDefinedAt(lab)) {
    //   println(lab)
    //   println(h.counts(lab))
    //   println(h.counts(left(lab)))
    //   println(h.counts(right(lab)))
    //   println("CUTTING NON-INTERNAL NODE!")
    //   println(result.leaves.toList.map(result.counts(_)).sum)
    //   System.exit(1)
    // } else if(!h.leaves.contains(left(lab)) && !h.leaves.contains(right(lab))) {
    //   println(lab)
    //   println(h.counts(lab))
    //   println(h.counts(left(lab)))
    //   println(h.counts(right(lab)))
    //   println("CUTTING WITH MISSING LEAVES!!!")
    //   println(result.leaves.toList.map(result.counts(_)).sum)
    //   System.exit(1)
    // } else if(!h.leaves.contains(left(lab))) {
    //   println(lab)
    //   println(h.counts(lab))
    //   println(h.counts(left(lab)))
    //   println(h.counts(right(lab)))
    //   println("CUTTING WITH MISSING LEFT LEAF!!!")
    //   println(left(lab))
    //   println(result.leaves.toList.map(result.counts(_)).sum)
    //   println(result.leaves.toList.filter(isAncestorOf(left(lab), _)))
    //   System.exit(1)
    // } else if(!h.leaves.contains(right(lab))) {
    //   println(lab)
    //   println(h.counts(lab))
    //   println(h.counts(left(lab)))
    //   println(h.counts(right(lab)))
    //   println("CUTTING WITH MISSING RIGHT LEAF!!!")
    //   println(right(lab))
    //   println(result.leaves.toList.map(result.counts(_)).sum)
    //   println(result.leaves.toList.filter(isAncestorOf(right(lab), _)))
    //   System.exit(1)
    // }
    result
  }

  def ancestors(x : NodeLabel) : Stream[NodeLabel] =
    Stream.iterate(x)(parent).takeWhile(_ >= rootLabel).tail

  def backtrack[H]( hs : Histogram,
                    prio : (Int, NodeLabel, Long, Double) => H)(implicit ord : Ordering[H])
      : Stream[(Histogram, Long, Long, Double, Double)] = {

    // Rewrite this in terms of PartialOrder[NodeLabel] and lexicographic order
    object BacktrackOrder extends Ordering[(H, NodeLabel)] {
      def compare(x : (H, NodeLabel), y : (H, NodeLabel)) = {
        val (xH, xLab) = x
        val (yH, yLab) = y
        if(isAncestorOf(xLab, yLab)) -1
        else if(isAncestorOf(yLab, xLab)) 1
        else ord.compare(xH, yH)
      }
    }

    var q = new PriorityQueue()(BacktrackOrder) //(Ordering[H].on({x : (H,NodeLabel) => x._1}))

    // TESTS
    // val s1 = depthFirst(hs.splits).map({ case (_, lab, _) => lab }).toSet

    // for(l <- hs.leaves) {
    //   for(a <- ancestors(l)) {
    //     if(!s1.any(isAncestorOf(_, a))) {
    //       println("Missing ancestor!!!")
    //       System.exit(1)
    //     }
    //   }
    // }

    // for(l <- s1) {
    //   if(!hs.leaves.any(isAncestorOf(l, _))) {
    //     println("Non-ancestral internal node!!!")
    //     System.exit(1)
    //   }
    // }

    // for(l <- hs.leaves) {
    //   if(hs.leaves.any(x => x != l && isAncestorOf(x, l))) {
    //     println("Non-leaf leaf!!!!")
    //     println(l)
    //     println(hs.leaves.filter(isAncestorOf(_, l)))
    //     System.exit(1)
    //   }
    // }

    depthFirst(hs.splits).foreach {
      case (lev, lab, _) =>
        q += ((prio(lev, lab, hs.counts(lab), hs.boxes(lab).volume), lab))
    }
    val sorted : Stream[(H, NodeLabel)] = q.dequeueAll

    // print(sorted.toList)

    sorted.scanLeft((hs, 0L, 0L, 0.0, 0.0)) {
      case ((hc, _, _, _, _), (_, lab)) =>
        ( cutCherry(hc, lab),
          hs.counts(left(lab)),
          hs.counts(right(lab)),
          hs.boxes(left(lab)).volume,
          hs.boxes(right(lab)).volume )
    }.tail
  }

  def logQuasiLik(h : Histogram) : Double =
    h.leaves.toList.map({
      case lab =>
        val c = h.counts(lab)
        if(c == 0) 0 else c*log(densityAtNode(h, lab))
    }).sum

  def logPenalisedQuasiLik(h : Histogram, taurec : Double) : Double =
    log(exp(taurec) - 1) - h.leaves.size*taurec + logQuasiLik(h)

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("ScalaDensity").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = normalVectorRDD(sc, 200, 2)

    val hs2 = histogram(df, 10, 0.0005)
    val hs1 = supportCarved(df, 5, 0.0005)

    val hs2path = backtrack(hs2, { case (_, _, count, _) => count })(Ordering[Long].reverse)

    def dumpHist(hs : Histogram, path : String) : Unit =
      Files.write(
        Paths.get(path),
        Vegas("Histogra").
          withData( df.collect().toSeq.map(x =>
                     Map("x" -> x(0), "y" -> x(1),
                         "d" -> density(hs, Vectors.dense(x(0), x(1))),
                         "n" -> query(hs, Vectors.dense(x(0), x(1)))
                     )
                   ) ).
          encodeX("x", Quant).
          encodeY("y", Quant).
          encodeOpacity("d", Quantitative).
          // encodeColor("n", Nom).
          mark(Point).
          html.pageHTML().getBytes(StandardCharsets.UTF_8))

    def dumpQuant(qs : Quantiles, path : String) : Unit =
      Files.write(
        Paths.get(path),
        Vegas("Histogra").
          withData( df.collect().toSeq.map(x =>
                     Map("x" -> x(0), "y" -> x(1),
                         "n" -> queryQuantile(qs, Vectors.dense(x(0), x(1)))
                     )
                   ) ).
          encodeX("x", Quant).
          encodeY("y", Quant).
          encodeOpacity("n", Quantitative).
          mark(Point).
          html.pageHTML().getBytes(StandardCharsets.UTF_8))

    var count = 1
    val temperature = 1.0
    dumpHist(hs2, "tmp0.html")
    for((hs, _, _, _, _) <- hs2path) {
      // TODO: implement an optimised version that recomputes the the error
      // based on the previous estimate and count/volume for the collapsed cells
      print("LOO error estimate:")
      println(looL2ErrorApprox(hs))
      print("Penalised Quasi-likelihood: ")
      println(exp(logPenalisedQuasiLik(hs, 1/temperature)))
      // Compute tail probabilities
      val qs = toQuantiles(hs)
      dumpQuant(qs, "tmpquant" + count.toString + ".html")
      print("Tail probability (0, 0): ")
      println(queryQuantile(qs, Vectors.dense(0.0, 0.0)))
      print("Tail probability (3, 3): ")
      println(queryQuantile(qs, Vectors.dense(3.0, 3.0)))
      dumpHist(hs, "tmp" + count.toString + ".html")
      count = count + 1
      println("")
    }



    // dumpHist(hs1, "tmp1.html")
    // dumpHist(hs2, "tmp2.html")
    sc.stop()
  }
}
