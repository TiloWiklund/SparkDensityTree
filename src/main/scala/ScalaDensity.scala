import scalaz._
import Scalaz._
// import co.theasi.plotly.{Plot, draw, ScatterOptions, ScatterMode}
// import scalax.chart.api._
import vegas._
// import vegas.render.WindowRenderer._

import scala.math.{min, max}
import scala.math.BigInt._

import scala.collection.mutable.HashMap
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

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

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
    Rectangle( ( b1.low, b2.low).zipped map min,
               (b1.high, b2.high).zipped map max )

  type InfiniteTree[T] = BigInt => T
  type FiniteTree[T] = Map[BigInt, T]
  // type PartialTree[T]  = InfiniteTree[Option[T]]
  // type MutableTree[T]  = HashMap[BigInt, T]

  // def unsafeFreeze[T](t : MutableTree[T])  : PartialTree[T] = t.get
  // def freeze[T](      t : MutableTree[T])  : PartialTree[T] = t.toMap.get
  // def toPartial[T](   t : InfiniteTree[T]) : PartialTree[T] = (x => Some(t(x)))
  // def restrict[T](    t : InfiniteTree[T], to : Set[BigInt]) : PartialTree[T] =
  //   x => if(to.apply(x)) some(t(x)) else none

  // def standardBasis(dim : Int) : Array[MLVector] =
  //   (1 to dim).map {
  //     i => MLVectors.dense(Array.fill(dim)(0.0).updated(i-1, 1.0))
  //   }.toArray

  def binarySplitTree(bbox : Rectangle) : InfiniteTree[(Rectangle, Double, Int)] = {
    val root : BigInt = 1
    val d = bbox.dim
    // val basis = standardBasis(d)

    // Tree that caches bounding box and depth of every node
    lazy val t : InfiniteTree[(Rectangle, Double, Int)] =
      Memo.mutableHashMapMemo {
        case `root` => (bbox, bbox.centre(0), 0)
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

  def left(x : BigInt)  : BigInt = 2*x
  def right(x : BigInt) : BigInt = 2*x + 1

  // def partitionMap[A, B](f : (A, B) => Boolean)(m : Map[A, B]) : (Map[A, B], Map[A, B]) = {
  //   val (l, r) = m.toTraversable.partition((x) => f(x._1, x._2))
  //   (l.toMap, r.toMap)
  // }

  case class Histogram( boxes  : FiniteTree[Rectangle],
                        splits : FiniteTree[(Double, Int)],
                        counts : FiniteTree[Long],
                        leaves : Set[BigInt] )

  // Recursive data types in Scala are a mess!
  // HistogramStream = Long -> (Histogram, HistogramStream)
  // In each step we have to provide a (possibly) new threshold
  // sealed trait HistogramStreamT[A <: HistogramStreamT[A]]
  // class HistogramStream[A <: HistogramStreamT[HistogramStream[A]]] extends HistogramStreamT[A] {
  //   next : Long => (Histogram, A)
  // }
  // Actually, with a single case case class we can avoid all the silliness
  case class HistogramStream(next : Long => (Histogram, Set[BigInt], HistogramStream))

  def histogramPartial(points : RDD[MLVector]) : HistogramStream = {
    val bbox = points.map(point(_)).reduce(hull)
    val fullTree = binarySplitTree(bbox)
    val boxTree = ((x : BigInt) => fullTree(x)._1)
    val splitsTree = { (x : BigInt) =>
      val (_, thresh, axis) = fullTree(x)
      (thresh, axis)
    }

    def go( positions : RDD[(BigInt, MLVector)],
            accCounts   : Map[BigInt, Long],
            accInternal : Set[BigInt],
            accLeaves   : Set[BigInt],
            accBoxes    : Map[BigInt, Rectangle],
            accSplits   : Map[BigInt, (Double, Int)] ) : HistogramStream =
      HistogramStream({ splitThresh : Long =>
                        // val currCounts = positions.mapValues(x => 1L).reduceByKeyLocally(_+_)
                        val currCounts = positions.countByKey()
                        // val (doSplit, dontSplit) = partitionMap((k : BigInt, v : Long) => v >= splitThresh)(currCounts.toMap)
                        val (doSplit, dontSplit) = currCounts.partition(_._2 > splitThresh)
                        val newCounts = accCounts ++ currCounts
                        val hist = Histogram(accBoxes, accSplits, newCounts, accLeaves)
                        ////
                        if(doSplit.isEmpty) {
                          (hist, Set(), go(positions, newCounts, accInternal, accLeaves, accBoxes, accSplits))
                        } else {
                          val newInternal = accInternal ++ doSplit.keySet
                          val oldLeaves   = doSplit.keySet
                          val addLeaves   = oldLeaves.flatMap(x => Array(left(x), right(x))).toSet
                          val newLeaves   = (accLeaves -- oldLeaves) ++ addLeaves
                          val newBoxes    = accBoxes ++ addLeaves.map(x => (x, boxTree(x)))
                          val addSplits   = doSplit.mapValues(x => splitsTree(x))
                          val newSplits   = accSplits ++ addSplits
                          // val newPositions   = positions.filter({ case (k, v) => doSplit.isDefinedAt(k) }).
                          //   map({ case (k, v) =>
                          //         val (thresh, axis) = newSplits(k)
                          //         if(v(axis) < thresh) {
                          //           (left(k), v)
                          //         } else {
                          //           (right(k), v)
                          //         }
                          //       }).localCheckpoint()
                          val newPositions = positions.
                            map({ case (k, v) =>
                                  val (thresh, axis) = newSplits(k)
                                  if(v(axis) < thresh) {
                                    (left(k), v)
                                  } else {
                                   (right(k), v)
                                  }
                                }).localCheckpoint()
                          (hist, addLeaves, go(newPositions, newCounts, newInternal, newLeaves, newBoxes, newSplits))
                        }
                      })

    return go(points.map((1,_)), Map(), Set(), Set(1), Map( BigInt(1) -> boxTree(1) ), Map())
  }

  def hist(points : RDD[MLVector], splitThresh : Long) : Histogram = {
    def go(hs : HistogramStream) : Histogram = {
      val (h, newLeaves, newHs) = hs.next(splitThresh)
      if(newLeaves.isEmpty)
        h
      else
        go(newHs)
    }
    go(histogramPartial(points))
  }

  def query(f : Histogram, p : MLVector) : BigInt = {
    var node = 1
    val splits = f.splits
    while(splits.isDefinedAt(node)) {
      val (t, d) = splits(node)
      if(p(d) <= t) {
        node = 2*node
      } else {
        node = 2*node + 1
      }
    }
    node
  }

  def density(f : Histogram, p : MLVector) : Double = {
    val node = query(f, p)
    f.counts(node)/f.boxes(node).volume
  }

  def queryDepth(f : InfiniteTree[(Rectangle, Double, Int)], d : Int, p : MLVector) : BigInt = {
    var node = 1
    for(i <- 2 to d) {
      val (_, t, d) = f(node)
      if(p(d) <= t) {
        node = 2*node
      } else {
        node = 2*node + 1
      }
    }
    node
  }

  // def histogram(points : RDD[MLVector], threshold : Int) : (Histogram, RDD[(Int, MLVector)]) = {
  //   val bboxtree   = root(u.map(point).reduce(hull))
  //   var bboxes     = Map((1, bboxtree))
  //   var leafcounts = Array((1, points.count()))
  //   var counts     = leafcounts.toMap
  //   var tree       = points.map(x => (1, x))
  //   var leafs      = Set(1)
  //   //
  //   var needsplit = Array(0)
  //   do {
  //     needsplit = leafcounts.filter(_._2 > threshold).map(_._1)
  //     leafs = leafs -- needsplit ++ needsplit.flatMap(x => Array(2*x, 2*x + 1))
  //     bboxes = bboxes ++ needsplit.flatMap(x => Array((2*x, bboxes(x).lower), (2*x + 1, bboxes(x).upper)))
  //     //println(leafs)
  //     //needsplit.foreach(print)
  //     //println("")
  //     val splits = needsplit.map(x => (x, (bboxes(x).splitAlong, bboxes(x).splitAt))).toMap
  //     //println(splits)
  //     tree = tree.map(n => if(splits contains n._1) {
  //                       val (along, at) = splits(n._1)
  //                       if(n._2(along) > at) { (2*n._1 + 1, n._2) }
  //                       else                 { (  2*n._1  , n._2) }
  //                     } else { n })
  //     leafcounts = leafCounts(tree)
  //     counts = counts ++ leafcounts
  //   } while (needsplit.length > 0);
  //   (Histogram(bboxtree, counts, leafs), tree)
  // }

  def main(args: Array[String]) = {
    // val (b,s) = binarySplitTree(Rectangle(Array(0, 0), Array(1, 1)))
    // println(b(1))
    // println(b(right(left(1))))
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("ScalaDensity").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = normalVectorRDD(sc, 100, 2)
    // df foreach println
    // print(df.map(point(_)).reduce(hull))

    val num = 1
    val hs = hist(df, 10)

    val boxes = binarySplitTree(df.map(point(_)).reduce(hull(_,_)))
    // print(boxes(1)._3)
    // print(boxes(left(1))._3)
    // print(boxes(left(left(1)))._3)
    // print(boxes(left(left(left(1))))._3)
    println(boxes(1)._1)
    println(boxes(left(1))._1)
    println(boxes(left(left(1)))._1)
    println(boxes(left(left(left(1))))._1)

    println(boxes(1)._2)
    println(boxes(left(1))._2)
    println(boxes(left(left(1)))._2)
    println(boxes(left(left(left(1))))._2)
    // println(h(1))
    // println(h(2))
    // println(h(3))

    // val p = Plot().withScatter(df.map(_(0)).collect(), df.map(_(1)).collect(), ScatterOptions().mode(ScatterMode.Marker))
    // draw(p, "test")
    // XYLineChart(df.collect().map(x => (x(0), x(1)))).show()

    // val data = df.collect().map(x => (x(0), x(1))).toTraversable
    // XYPlot(data).show()

    // for(i <- 1 to 10) {
    //   val h = hs(i)
      Files.write(
        // Paths.get("tmp" + i + ".html"),
        Paths.get("tmp.html"),
        Vegas("Example").
          withData( df.collect().toSeq.map(x => Map("x" -> x(0), "y" -> x(1), "d" -> density(hs, Vectors.dense(x(0), x(1))), "n" -> query(hs, Vectors.dense(x(0), x(1))) )) ).
          encodeX("x", Quantitative).
          encodeY("y", Quantitative).
          // encodeColor("d", Quantitative).
          encodeColor("nn", Nominal).
          mark(Point).
          // withData( h.leaves.toArray.map( { x => val i = 1; Map( "x" -> -1, "y" -> -1, "x2" -> 1, "y2" -> 1) } )).
          // encodeX("x", Quantitative).
          // encodeY("y", Quantitative).
          // encodeX2("x2", Quantitative).
          // encodeY2("y2", Quantitative).
          // mark(Area).
          html.pageHTML().getBytes(StandardCharsets.UTF_8))
    // }
    sc.stop()
  }
}
