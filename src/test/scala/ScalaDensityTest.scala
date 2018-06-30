import scala.language.postfixOps

import ScalaDensity._
import org.apache.spark.mllib.linalg.{ Vector => MLVector, _ }
import scala.math.{abs, pow}

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Logger, Level }
import org.apache.spark.mllib.random.RandomRDDs.normalVectorRDD

import org.scalatest.{ path => testPath, _ }
import org.scalactic.TolerantNumerics

class DensityTests extends FlatSpec with Matchers with BeforeAndAfterAll {
  // "it" should "compile" in {
  //   assert(1 === 1)
  // }
  private var sc : SparkContext = null
  private var df : RDD[MLVector] = null
  private var bb : Rectangle = null
  private var h : Histogram = null

  private val dfnum = 5000
  private val dfdim = 3
    // val df = normalVectorRDD(sc, n, 2)

  def lims(totalVolume : Double, totalCount : Count)(depth : Int, volume : Volume, count : Count) =
    count > dfnum/2 || (1 - count/totalCount)*volume > 0.001*totalVolume

  override protected def beforeAll() : Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("ScalaDensityTest").setMaster("local")
    sc = new SparkContext(conf)
    df = normalVectorRDD(sc, dfnum, dfdim, 6, 7387389).cache()
    bb = boundingBox(df)
    h = histogram(df, lims, noEarlyStop)
  }

  override protected def afterAll() : Unit = {
    df.unpersist()
    sc.stop()
    sc = null
    df = null
    bb = null
  }

  "binarySearch" should "find first true value" in {
    assert(binarySearch((x : Int) => x >= 3)(Vector(0, 1, 2, 3, 3, 4, 5)) === 3)
  }

  "Rectangle" should "preserve total volume when split" in {
    for(i <- 0 until dfdim)
      assert(abs(bb.volume-bb.lower(i).volume-bb.upper(i).volume) < 0.0000001)
  }

  it should "split into equal volumes" in {
    for(i <- 0 until dfdim)
      assert(abs(bb.lower(i).volume - bb.upper(i).volume) < 0.0000001)
  }

  it should "have expected dimension" in {
    assert(bb.dimension === dfdim)
  }

  "single point" should "have volume 0" in {
    assert(point(df.first()).volume === 0)
  }

  "boundingBox" should "contain all points" in {
    val bb = boundingBox(df)
    assert(df.map(bb.contains(_)).reduce(_ && _))
  }

  "root node" should "have depth 0" in {
    assert(rootLabel.depth === 0)
  }

  "node" should "have its left/right leaf on the left/right" in {
    val node = NodeLabel(49)
    assert(node.left.isLeft)
    assert(node.right.isRight)
  }

  it should "be parent to its children" in {
    val node = NodeLabel(38)
    assert(node.children.forall(_.parent == node))
  }

  it should "have an ancestry consisting exactly of its ancestors" in {
    val node = NodeLabel(232)
    val ancestry1 = node.ancestors.toSet
    val ancestry2 = (1 until 232).
      map(NodeLabel(_)).
      filter(isAncestorOf(_, node)).
      toSet
    assert(ancestry1 === ancestry2)
  }

  // "new left/right ordering" should "agree with the old one" in {
  //   def isAncestorOfOld(a : NodeLabel, b : NodeLabel) : Boolean =
  //     a.lab < b.lab && (b.ancestor(b.depth - a.depth) == a)

  //   def isLeftOfOld(a : NodeLabel, b : NodeLabel) : Boolean =
  //     a.truncate(b.depth).lab < b.truncate(a.depth).lab

  //   val l = rootLabel.left
  //   val r = rootLabel.right
  //   val lll = l.left.left

  //   assert(isAncestorOfOld(l, lll) === isAncestorOf(l, lll))
  //   assert(isAncestorOfOld(l, r) === isAncestorOf(l, r))
  //   assert(isLeftOfOld(l, lll) === isLeftOf(l, lll))
  //   assert(isLeftOfOld(l, r) === isLeftOf(l, r))
  // }

  "MRSName" should "work on test cases" in {
    val lrllr = rootLabel.left.right.left.left.right
    assert(rootLabel.mrsName === "X")
    assert(lrllr.mrsName === "XLRLLR")
  }

  "join" should "be a supremum" in {
    val j = rootLabel.left.right.right
    val a = j.left.right.right.left.right.right
    val b = j.right.right.left.left.right
    assert(j == join(a, b))
    assert(a == join(a, a))
    assert(b == join(b, b))
    assert(j == join(j, a))
    assert(j == join(j, b))
  }

  "initialLefts" should "give number of initial left steps" in {
    val lab1 = rootLabel.left.left.right
    val lab2 = rootLabel.left.left.left.right.left
    assert(rootLabel.initialLefts === 0)
    assert(lab1.initialLefts === 2)
    assert(lab2.initialLefts === 3)
  }

  "initialRights" should "give number of initial right steps" in {
    val lab1 = rootLabel.right.right.left
    val lab2 = rootLabel.right.right.right.left.right
    assert(rootLabel.initialRights === 0)
    assert(lab1.initialRights === 2)
    assert(lab2.initialRights === 3)
  }

  // "leftmostAncestor" should "be youngest leftmost ancestor" in {
  //   val lab = rootLabel.left.left.right.right.left
  //   assert(isAncestorOf(lab.leftmostAncestor, lab))
  //   assert(!isAncestorOf(lab.leftmostAncestor.left, lab))
  // }

  // "rightmostAncestor" should "be youngest rightmost ancestor" in {
  //   val lab = rootLabel.right.right.left.left.right
  //   assert(isAncestorOf(lab.rightmostAncestor, lab))
  //   assert(!isAncestorOf(lab.rightmostAncestor.right, lab))
  // }

  "path" should "be a sorted open interval" in {
    val start = rootLabel.left.right.left.left.left.right
    val stop  = rootLabel.right.left.left.right
    val p = path(start, stop)
    p.sliding(2).foreach(x => assert(adjacent(x(0), x(1))))
    assert(adjacent(p.head, start))
    assert(adjacent(p.last, stop))
    assert(path(start, start).isEmpty)
    assert(path(stop, stop).isEmpty)
  }

  it should "be a walk" in {
    val start = rootLabel.left.right.left.left.left.right
    val stop  = rootLabel.right.left.left.right
    val p = path(start, stop)
    p.sliding(2).forall {
      case x => adjacent(x(0), x(1))
    }
  }

  "truncation" should "have size 1 when truncated at root" in {
    assert(rootTruncation.allNodes.size === 1)
  }

  it should "have size 1 at a leaf" in {
    val trunc = fromLeafSet(Set(rootLabel.left, rootLabel.right))
    assert(trunc.subtree(rootLabel.left) === Subset(0,1))
    assert(trunc.subtree(rootLabel.right) === Subset(1,2))
  }

  it should "have leaves in left/right order" in {
    val trunc = fromLeafSet(Set(rootLabel.right, rootLabel.left))
    isLeftOf(trunc.leaves(0), trunc.leaves(1))
  }

  it should "have completion in left/right order" in {
    // def lims(tv : Volume, tc : Count)(d : Int, v : Volume, c : Count) : Boolean =
    //   c > 100 || (1 - c/tc)*v/tv > 0.1

    // val h = histogram(df, lims, noEarlyStop)

    h.counts.minimalCompletionNodes.sliding(2).foreach {
      case ((llab, _) #:: (rlab, _) #:: _) =>
        isLeftOf(llab, rlab)
    }
  }

  it should "have completion be a finite tree" in {
    // TODO: Something is wrong with this test, didn't catch a bad bug!

    val leaves1 = Set(rootLabel.left.left.right, rootLabel.right.left)
    val trunc = fromLeafSet(leaves1)
    val leaves = trunc.minimalCompletion.leaves.toSet
    // print("Start: ")
    // println(leaves1)
    // print("Completed: ")
    // println(leaves)

    leaves.foreach {
      case l =>
        assert(!leaves.exists(isAncestorOf(_,l)))
    }

    val d = leaves.map(_.depth).max + 1
    var nodes = Set(rootLabel)
    for(i <- 1 to d) {
      // print("N: ")
      // println(nodes)
      nodes = (nodes -- leaves).flatMap(x => Set(x.left, x.right))
    }
    assert(nodes === Set())
  }

  "unfoldTree" should "interprets nodelabels correctly" in {
    val t = unfoldTree(rootLabel)((_, lab) => lab.left, (_, lab) => lab.right)(_)
    List(rootLabel, rootLabel.left, rootLabel.right, rootLabel.left.left, rootLabel.right.left).foreach {
      case lab =>
        assert(t(lab) === lab)
    }
  }

  "cached unfold" should "should agree with uncached" in {
    val tree = spatialTreeRootedAt(bb)

    val l  = rootLabel.left
    val rl = rootLabel.right.left
    val rr = rootLabel.right.right
    val ll = rootLabel.left.left
    val lr = rootLabel.left.right
    val c  = tree.cellAtCached.recache(Set(l, rl, rr))
    for(lab <- List(rootLabel, l, rl, rr, ll, lr)) {
      assert(c(lab).low === tree.cellAt(lab).low)
      assert(c(lab).high === tree.cellAt(lab).high)
    }
  }

  "leafMap" should "agree with Map on values" in {
    val l = rootLabel.left
    val r = rootLabel.right
    val m1 = Map(l -> 1, r -> 2)
    val m2 = fromNodeLabelMap(m1)
    assert(m2.query(rootLabel #:: l #:: Stream.empty)._2 === m1.get(l))
    assert(m2.query(rootLabel #:: r #:: Stream.empty)._2 === m1.get(r))
  }

  it should "have leaves in left/right order" in {
    val l = rootLabel.left
    val r = rootLabel.right
    val m1 = Map(l -> 1, r -> 2)
    val m2 = fromNodeLabelMap(m1)
    isLeftOf(m2.truncation.leaves(0), m2.truncation.leaves(1))
  }

  it should "have toMap as inverse to fromNodeLabelMap" in {
    val l = rootLabel.left
    val r = rootLabel.right
    val m1 = Map(l -> 1, r -> 2)
    val m2 = fromNodeLabelMap(m1)
    assert(m1 === m2.toMap)
  }

  "spatialTree" should "have bounding box at root" in {
    val tree = spatialTreeRootedAt(bb)
    assert(tree.cellAt(rootLabel) === bb)
  }

  it should "first split on first coordinate" in {
    val tree = spatialTreeRootedAt(bb)
    assert(tree.axisAt(rootLabel) === 0)
  }

  it should "actually split along the right axes" in {
    val tree = spatialTreeRootedAt(bb)
    assert(tree.cellAt(rootLabel.left).centre(0) != tree.cellAt(rootLabel).centre(0))
    assert(tree.cellAt(rootLabel.left.left).centre(1) != tree.cellAt(rootLabel.left).centre(1))
    assert(tree.cellAt(rootLabel.left.left.left).centre(2) != tree.cellAt(rootLabel.left.left).centre(2))
  }

  "descendSpatialTree" should "always terminate" in {
    val tree = spatialTreeRootedAt(bb)
    val trunc = rootTruncation
    val x = df.takeSample(true, 1).head
    assert(rootLabel === trunc.descendUntilLeaf(tree.descendBox(x)))
  }

  "descendBox" should "remain in cell containing point" in {
    val tree = spatialTreeRootedAt(bb)
    val x = df.takeSample(true, 1).head
    val walk = tree.descendBox(x).take(10)
    var i = 0
    walk.foreach {
      case lab =>
        val cell = tree.cellAt(lab)
        assert(cell.contains(x))
    }
  }

  it should "have boxes aggreing with cellAt" in {
    val tree = spatialTreeRootedAt(bb)
    val x = df.takeSample(true, 1).head
    val walk = tree.descendBoxPrime(x).take(10)
    walk.foreach {
      case (lab, box1) =>
        val box2 = tree.cellAt(lab)
        assert(box1.low === box2.low)
        assert(box1.high === box2.high)
    }
  }

  def assertdistinct[A](a : Seq[A]) : Unit = {
    a.tails.foreach {
      case Seq(x, xs@_*) => xs.foreach {
        case y => assert(y != x)
      }
      case Seq() => ()
    }
  }

  "cherries" should "all be cherries" in {
    h.counts.cherries(_+_).foreach {
      case (lab, _) => assert(h.counts.truncation.hasAsCherry(lab))
    }
  }

  it should "give distinct ones" in {
    // println(h.counts.cherries(_+_).map(_._1).toVector)
    assertdistinct(h.counts.cherries(_+_).map(_._1).toSeq)
  }

  "splitAndCountFrom" should "have only non-splittable boxes and splittable parents" in {
    val tree = spatialTreeRootedAt(bb)
    val counts = splitAndCountFrom(tree, rootTruncation, df, lims, noEarlyStop)

    for((l, c) <- counts) {
      assert(!lims(tree.volumeAt(rootLabel), counts.values.sum)(l.depth, tree.volumeAt(l), c))
    }

    val parents : Map[NodeLabel, Count] = counts.keySet.map(_.parent).map(x => (x, counts getOrElse (x, 0L))).toMap
    for((l, c) <- parents) {
      assert(lims(tree.volumeAt(rootLabel), counts.values.sum)(l.depth, tree.volumeAt(l), c))
    }
  }

  it should "compute correct cell counts" in {
    def lims(tv : Volume, tc : Count)(d : Int, v : Volume, c : Count) : Boolean =
      c > 100 || (1 - c/tc)*v/tv > 0.1

    val tree = spatialTreeRootedAt(bb)
    val counts = splitAndCountFrom(tree, rootTruncation, df, lims, noEarlyStop)

    for((l, c) <- counts) {
      val b = tree.cellAt(l)
      assert(c === df.filter(b.contains(_)).count)
    }
  }

  "internal" should "finds all internal nodes" in {
    val t = fromNodeLabelMap(List(rootLabel.left.left.right,
                                  rootLabel.left.right.right,
                                  rootLabel.right.right.left).map((_, ())).toMap)
    val internals1 = (BigInt(1) to t.truncation.leaves.map(_.lab).max).
      map(NodeLabel(_)).
      filter(x => t.truncation.leaves.exists(isAncestorOf(x, _))).
      toSet
    val internals2 = t.
      internal((), (_ : Unit, _ : Unit) => ()).
      map(_._1).
      toSet
    assert(internals1 === internals2)
  }

  it should "accumulate correct values" in {
    val t = fromNodeLabelMap(List(rootLabel.left.left.right,
                                  rootLabel.left.right.right,
                                  rootLabel.right.right.left).map(x => (x, Set(x))).toMap)

    val internals1 = (BigInt(1) to t.truncation.leaves.map(_.lab).max).
      map(NodeLabel(_)).
      filter(x => t.truncation.leaves.exists(isAncestorOf(x, _))).
      map(lab => (lab, t.slice(t.truncation.subtree(lab)).reduce(_.union(_)))).
      toSet

    val internals2 = t.
      internal(Set.empty, _.union(_)).
      toSet

    assert(internals1 === internals2)
  }

  "backtrack" should "traverses all ancestors in correct order" in {
    def prio(lab : NodeLabel, c : Count, v : Volume) : Count = c
    // def lims(tv : Volume, tc : Count)(d : Int, v : Volume, c : Count) : Boolean =
    //   c > 100 || (1 - c/tc)*v/tv > 0.1
    // val h = histogram(df, lims, noEarlyStop)

    // def go(xs : Stream[(NodeLabel, Count)]) : Boolean = cs match {
    //   case Stream.Empty => true
    //   case ((lab, c) #:: xss) => (vs.forall { case (lab2, c2) => isAncestorOf(lab2, lab) }) && go(xss)
    // }
    // val nrinternals = (BigInt(1) to h.counts.truncation.leaves.map(_.lab).max).
    //   map(NodeLabel(_)).
    //   filter(x => h.counts.truncation.leaves.exists(isAncestorOf(x, _))).
    //   size

    val bt = h.backtrackWithNodes(prio).toVector.toStream

    bt.tails.foreach {
      case Stream.Empty => ()
      case ((lab1, h1) #:: rest) =>
        rest.foreach {
          case (lab2, _) => assert(!isAncestorOf(lab1, lab2))
        }
        // rest.foreach {
        //   case (lab2, h2) => assert(isAncestorOf(lab2, lab1) || c1 <= c2)
        // }
        rest.exists {
          case (lab2, _) => lab2 === lab1.parent
        }
    }
  }

  it should "only traverse ancestors" in {
    def prio(lab : NodeLabel, c : Count, v : Volume) : Count = c

    h.backtrackNodes(prio).foreach {
      case n =>
        h.counts.truncation.leaves.exists {
          case n2 =>
            isAncestorOf(n, n2)
        }
    }
  }

  it should "traverse everything once" in {
    def prio(lab : NodeLabel, c : Count, v : Volume) : Count = c
    val tracked = h.backtrackNodes(prio).toVector.toStream
    assertdistinct(tracked)
  }

  it should "end in trivial histogram" in {
    def prio(lab : NodeLabel, c : Count, v : Volume) : Count = c

    assert(h.backtrack(prio).toStream.last.counts.truncation.leaves === Vector(rootLabel))
  }

  it should "traverse all ancestors" in {
    def prio(lab : NodeLabel, c : Count, v : Volume) : Count = c

    // h.backtrack(prio).toVector.reverse.take(10).foreach(x=>println(x.counts.truncation.leaves))

    val tracked = h.backtrackNodes(prio).toSet
    val full = h.counts.truncation.leaves.toSet.flatMap((x : NodeLabel) => x.ancestors())
    assert((tracked -- full).isEmpty)
    assert((full -- tracked).isEmpty)
    // h.counts.truncation.leaves.foreach {
    //   case c =>
    //     c.ancestors.foreach {
    //       case a =>
    //         assert(tracked(a))
    //     }
    // }
  }

  it should "remove the correct leaf node" in {
    def prio(lab : NodeLabel, c : Count, v : Volume) : Count = c

    h.backtrackWithNodes(prio).toStream.sliding(2).toStream.foreach {// .zip(h.backtrackNodes(prio).sliding(2).toStream).foreach {
      case ((n1, h1) #:: (n2, _) #:: _) =>
        assert(h1.counts.truncation.leaves.contains(n1))
        assert(!h1.counts.truncation.leaves.contains(n1.left))
        assert(!h1.counts.truncation.leaves.contains(n1.right))
        assert(!h1.counts.truncation.leaves.contains(n2))
        assert(h1.counts.truncation.leaves.contains(n2.left) || h1.counts.truncation.leaves.contains(n2.right))
    }
  }

  it should "give histograms of decreasing size" in {
    def prio(lab : NodeLabel, c : Count, v : Volume) : Count = c

    // def go(xs : Stream[(NodeLabel, Count)]) : Boolean = cs match {
    //   case Stream.Empty => true
    //   case ((lab, c) #:: xss) => (vs.forall { case (lab2, c2) => isAncestorOf(lab2, lab) }) && go(xss)
    // }
    // h.backtrack(prio).foreach {
    //   case x =>
    //     print("Backtrack Result: ")
    //     println(x.truncation.leaves.toList)
    // }
    // h.backtrack(prio).toStream.sliding(2).zip(h.backtrackNodes(prio).toStream.tail).foreach {
    h.backtrack(prio).toStream.sliding(2).foreach {
      case (h1 #:: h2 #:: rest) =>
        // print(h1.truncation.leaves.toList)
        // print("--")
        // println(h2.truncation.leaves.toList)
        val diff1 = h1.truncation.leaves.toSet -- h2.truncation.leaves.toSet
        val diff2 = h2.truncation.leaves.toSet -- h1.truncation.leaves.toSet
        // println(diff1)
        // println(diff2)

        // if(diff1.size != 1 && diff1.size != 2) {
        //   println("----------------------")
        //   println(h1.counts.truncation.subtree(lab))
        //   println("----------------------")
        //   println(h1.counts.truncation.leaves.filter(isAncestorOf(lab, _)))
        //   println("--------------------!!")
        //   println(h.counts.truncation.leaves.filter(isAncestorOf(lab, _)))
        //   println("----------------------")
        //   println(lab)
        //   println("----------------------")
        //   println(diff1)
        //   println("----------------------")
        //   println(diff2)
        //   println("----------------------")
        //   println(h1)
        //   println("----------------------")
        //   println(h2)
        //   println("----------------------")
        // }

        assert(diff1.size === 1 || diff1.size === 2)
        assert(diff2.size === 1)
        assert(diff1.forall(x => isAncestorOf(diff2.toVector(0), x)))
        // assert(diff2.forall(x => diff1.exists(y => isAncestorOf(x, y))))
        // // Extra sanity check, should be same as above
        // assert(h1.truncation.leaves.size <= h2.truncation.leaves.size + 1)
        // assert(h1.ncells <= h2.ncells + 1)
    }
  }

  "mergeSubtree" should "contain the correct leaves" in {
    val t = fromNodeLabelMap(Map(rootLabel.left.left.left    -> 1,
                                 rootLabel.left.left.right   -> 2,
                                 rootLabel.left.right.left   -> 3,
                                 rootLabel.left.right.right  -> 4,
                                 rootLabel.right.left.left   -> 5,
                                 rootLabel.right.left.right  -> 6,
                                 rootLabel.right.right.left  -> 7,
                                 rootLabel.right.right.right -> 8 ))
    val l = rootLabel.left
    val m = t.mergeSubtree(l, _ + _)

    val n = m.leaves.toSet -- t.leaves.toSet
    val d = t.leaves.toSet -- m.leaves.toSet

    assert(n == Set(l))
    d.foreach(x => assert(isAncestorOf(l, x)))
  }

  it should "contain the merged node" in {
    val t = fromNodeLabelMap(Map(rootLabel.left.left.left    -> 1,
                                 rootLabel.left.left.right   -> 2,
                                 rootLabel.left.right.left   -> 3,
                                 rootLabel.left.right.right  -> 4,
                                 rootLabel.right.left.left   -> 5,
                                 rootLabel.right.left.right  -> 6,
                                 rootLabel.right.right.left  -> 7,
                                 rootLabel.right.right.right -> 8 ))
    val l = rootLabel.left
    val m = t.mergeSubtree(l, _ + _)
    assert(!t.truncation.leaves.contains(l))
    assert(m.truncation.leaves.contains(l))
  }

  "tailProbabilities" should "should be probabilities, attaining maximum 1" in {
    val tp = h.tailProbabilities()
    tp.tails.vals.foreach {
      case p =>
        assert(0 <= p)
        assert(p <= 1)
    }
    assert(tp.tails.vals.max === 1)
  }

  it should "have tail 0/1 in minimum/maximum density cell" in {
    val tp   = h.tailProbabilities()
    val imax = tp.tails.vals.zipWithIndex.maxBy(_._1)._2
    val imin = tp.tails.vals.zipWithIndex.minBy(_._1)._2
    val cmax = h.counts.vals(imax)
    val cmin = h.counts.vals(imin)
    assert(cmax === h.counts.vals.max)
    assert(cmin === h.counts.vals.min)
  }
}
