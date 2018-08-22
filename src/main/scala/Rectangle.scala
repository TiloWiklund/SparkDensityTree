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

import scala.math.{min, max, exp, log, pow, ceil}

import Types._

// Axis parallel (bounding) boxes

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

object RectangleFunctions {
  def hull(b1 : Rectangle, b2 : Rectangle) : Rectangle =
    Rectangle( ( b1.low, b2.low ).zipped map min,
               (b1.high, b2.high).zipped map max )

  def point(v : MLVector) : Rectangle =
    Rectangle(v.toArray.toVector, v.toArray.toVector)

  def boundingBox(vs : RDD[MLVector]) : Rectangle =
    vs.map(point(_)).reduce(hull)
}

import RectangleFunctions._
