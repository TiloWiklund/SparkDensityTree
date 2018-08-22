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

import Types._

import NodeLabelFunctions._

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

object UnfoldTreeFunctions {
  def unfoldTree[A](base : A)(left : (NodeLabel, A) => A, right : (NodeLabel, A) => A)(lab : NodeLabel) : A = {
    if(lab == rootLabel) {
      base
    } else if(lab.isLeft) {
      left(lab, unfoldTree(base)(left, right)(lab.parent))
    } else {
      right(lab, unfoldTree(base)(left, right)(lab.parent))
    }
  }

  def unfoldTreeCached[A](base : A)(left : (NodeLabel, A) => A, right : (NodeLabel, A) => A) : CachedUnfoldTree[A] =
    CachedUnfoldTree(base, Map.empty, left, right)
}

import UnfoldTreeFunctions._
