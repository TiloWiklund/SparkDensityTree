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

// (The) infinite binary tree

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

object NodeLabelFunctions {
  val rootLabel : NodeLabel = NodeLabel(1)

  def join(a : NodeLabel, b : NodeLabel) : NodeLabel = {
    val d = min(a.depth, b.depth)
    val aT = a.truncate(d)
    val bT = b.truncate(d)
    aT.ancestor((aT.lab ^ bT.lab).bitLength)
  }

  def isAncestorOf(a : NodeLabel, b : NodeLabel) : Boolean =
    a.lab < b.lab && b.truncate(a.depth) == a
    // a.lab < b.lab && leftRightOrd.compare(a, b) == 0

  def isDescendantOf(a : NodeLabel, b : NodeLabel) : Boolean =
    isAncestorOf(b, a)

  def isStrictLeftOf(a : NodeLabel, b : NodeLabel) : Boolean =
    a.truncate(b.depth).lab < b.truncate(a.depth).lab
  // leftRightOrd.compare(a, b) < 0

  def isLeftOf(a : NodeLabel, b : NodeLabel) : Boolean = a == b || isStrictLeftOf(a, b)
  //   if(a == b) True
  //   else {
  //     val j = join(a, b)
  //     if(inLeftSubtreeOf(a, j) || inRightSubtreeOf(b, j)) -1
  //     else 1
  //   }
  // }
    // leftRightOrd.compare(a, b) < 0

  def isRightOf(a : NodeLabel, b : NodeLabel) : Boolean =
    !isStrictLeftOf(a, b)
    // leftRightOrd.compare(a, b) > 0

  def isStrictRightOf(a : NodeLabel, b : NodeLabel) : Boolean =
    !isLeftOf(b, a)

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

  type Walk = Stream[NodeLabel]
}

import NodeLabelFunctions._

object leftRightOrd extends Ordering[NodeLabel] {
  def compare(a : NodeLabel, b : NodeLabel) =
    if(a == b) 0 else if(isLeftOf(a, b)) -1 else +1
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
