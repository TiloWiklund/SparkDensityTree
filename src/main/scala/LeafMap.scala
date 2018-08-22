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

import scala.reflect.ClassTag

import Types._

import NodeLabelFunctions._

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

object LeafMapFunctions {
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
}

import LeafMapFunctions._
