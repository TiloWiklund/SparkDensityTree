import scalaz.{ Tree => _ , _ }
import Scalaz._

import scala.math.{BigInt, min, max}

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.random.RandomRDDs.normalVectorRDD

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{ Logger, Level }

object ScalaDensity {
  case class Rectangle(low : Array[Double], high : Array[Double]) {
    override def toString = (low, high).zipped.toArray.mkString("{ ", ", ", " }")

    def dim() = high.length

    def centre(along : Int) : Double =
      (high(along) + low(along))/2

    def split(along : Int) : (Rectangle, Rectangle) = {
      val c = centre(along)
      (Rectangle(low,                   high.updated(along, c)),
       Rectangle(low.updated(along, c), high                  ))
    }

    def lower(along : Int) : Rectangle = split(along)._1

    def upper(along : Int) : Rectangle = split(along)._2

    def volume() : Double =
      ((high, low).zipped map (_-_)) reduce (_*_)
  }

  def point(v : Vector) : Rectangle = Rectangle(v.toArray, v.toArray)

  def hull(b1 : Rectangle, b2 : Rectangle) : Rectangle =
    Rectangle((b1.low, b2.low).zipped map min, (b1.high, b2.high).zipped map max)

  type Tree[T] = BigInt => T
  type FiniteTree[T] = Tree[Option[T]]

  // case class Tree[T](value : T, children : LazyTuple2[Tree[T], Tree[T]])

  // def tree[T](value : T, left : => Tree[T], right : => Tree[T]) : Tree[T] =
  //   Tree(value, LazyTuple.lazyTuple2(left, right))

  // def goLeft[T](t : Tree[T])  = t.children._1
  // def goRight[T](t : Tree[T]) = t.children._2

  // object Direction extends Enumeration {
  //   type Direction = Value
  //   val Left, Right = Value
  //   override def toString : String = this match {
  //     case Left => "Left"
  //     case Right => "Right"
  //   }
  // }

  // import Direction._

  // def descend[T](path : Stream[Direction], t : Tree[T]) : Stream[Tree[T]] = {
  //   def go(tP : Tree[T], d : Direction) : Tree[T] =
  //     if(d == Left) goLeft(tP) else goRight(tP)
  //   path.scanLeft(t)(go)
  // }

  // def toCanonical(path : Stream[Direction]) : BigInt =
  //   path.foldLeft(0)((acc, dir) => if(dir == Left) acc << 1 else (acc << 1) + 1)

  // def fromCanonical(idx : BigInt) : Stream[Direction] =
  //   unfold(idx)(x => (x =/= 0) option (if((x & 1) == 0) (Left, x >> 1) else (Right, x >> 1)))

  // def plusmod(n : Int, m : Int, mod : Int) : Int =
  //   (n + m) % mod

  // def axisAlignedTree(bbox : Rectangle) : Tree[Rectangle] = {
  //   val d = dim(bbox)
  //   def go(along : Int, bboxInner : Rectangle) : Tree[Rectangle] = {
  //     val (lv, uv) = split(along, bboxInner)
  //     lazy val lt = go(plusmod(along, 1, d), lv)
  //     lazy val ut = go(plusmod(along, 1, d), uv)
  //     tree(bboxInner, lt, ut)
  //   }
  //   go(0, bbox)
  // }

  def main(args: Array[String]) = {
    val r = Rectangle(Array(0, 0), Array(1, 1))
    println(r)
    // Logger.getLogger("org").setLevel(Level.ERROR)
    // Logger.getLogger("akka").setLevel(Level.ERROR)

    // val conf = new SparkConf().setAppName("ScalaDensity").setMaster("local[2]")
    // val sc = new SparkContext(conf)
    // val sqlContext = new SQLContext(sc)
    // import sqlContext.implicits._

    // val df = normalVectorRDD(sc, 20, 2)
    // df foreach println

    // sc.stop()
  }
}
