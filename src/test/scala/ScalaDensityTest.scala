import scala.language.postfixOps

import org.scalatest.{ path => testPath, _ }

import ScalaDensity._
import org.apache.spark.mllib.linalg.{ Vector => MLVector, _ }
import scala.math.{abs, pow}

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Logger, Level }
import org.apache.spark.mllib.random.RandomRDDs.normalVectorRDD

class DensityTests extends FlatSpec with Matchers with BeforeAndAfterAll {
  // "it" should "compile" in {
  //   assert(1 === 1)
  // }
  private var sc : SparkContext = null
  private var df : RDD[MLVector] = null
  private var bb : Rectangle = null

  private val dfnum = 200
  private val dfdim = 3

  override protected def beforeAll() : Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("ScalaDensityTest").setMaster("local")
    sc = new SparkContext(conf)
    df = normalVectorRDD(sc, dfnum, dfdim).persist()
    bb = boundingBox(df)
  }

  override protected def afterAll() : Unit = {
    df.unpersist()
    sc.stop()
    sc = null
    df = null
    bb = null
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

  "new left/right ordering" should "agree with the old one" in {
    def isAncestorOfOld(a : NodeLabel, b : NodeLabel) : Boolean =
      a.lab < b.lab && (b.ancestor(b.depth - a.depth) == a)

    def isLeftOfOld(a : NodeLabel, b : NodeLabel) : Boolean =
      a.truncate(b.depth).lab < b.truncate(a.depth).lab

    val l = rootLabel.left
    val r = rootLabel.right
    val lll = l.left.left

    assert(isAncestorOfOld(l, lll) === isAncestorOf(l, lll))
    assert(isAncestorOfOld(l, r) === isAncestorOf(l, r))
    assert(isLeftOfOld(l, lll) === isLeftOf(l, lll))
    assert(isLeftOfOld(l, r) === isLeftOf(l, r))
  }

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

  "path" should "be an open interval" in {
    val start = rootLabel.left.right.left.left.left.right
    val stop  = rootLabel.right.left.left.right
    val p = path(start, stop)
    assert(p === p.sorted(leftRightOrd))
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
    def lims(tv : Volume, tc : Count)(d : Int, v : Volume, c : Count) : Boolean =
      c > 100 || (1 - c/tc)*v/tv > 0.1

    val h = histogram(df, lims)

    h.counts.minimalCompletionNodes.sliding(2).foreach {
      case ((llab, _) #:: (rlab, _) #:: _) =>
        isLeftOf(llab, rlab)
    }
  }

  it should "have completion be a finite tree" in {
    // TODO: Change this to some determinstic tree instead of hist!
    def lims(tv : Volume, tc : Count)(d : Int, v : Volume, c : Count) : Boolean =
      c > 100 || (1 - c/tc)*v/tv > 0.1

    val h = histogram(df, lims)

    val leaves = h.counts.truncation.leaves.toSet
    val d = leaves.map(_.depth).max + 1
    var nodes = Set(rootLabel)
    for(i <- 1 to d) {
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
      assert(c(lab).low.deep === tree.cellAt(lab).low.deep)
      assert(c(lab).high.deep === tree.cellAt(lab).high.deep)
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
        assert(box1.low.deep === box2.low.deep)
        assert(box1.high.deep === box2.high.deep)
    }
  }

  "splitAndCountFrom" should "have only non-splittable boxes and splittable parents" in {
    def lims(tv : Volume, tc : Count)(d : Int, v : Volume, c : Count) : Boolean =
      c > 100 || (1 - c/tc)*v/tv > 0.1

    val tree = spatialTreeRootedAt(bb)
    val counts = splitAndCountFrom(tree, rootTruncation, df, lims)

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
    val counts = splitAndCountFrom(tree, rootTruncation, df, lims)

    for((l, c) <- counts) {
      val b = tree.cellAt(l)
      assert(c === df.filter(b.contains(_)).count)
    }
  }

  // "descend" should "be a decreasing sequence termining in empty" in {

  // }

  // "rootTree" should "have root node as leaf" in {
  //   val tree = rootTree(())
  //   assert(tree.hasLeaf(rootLabel))
  // }

  // it should "replace root node after split" in {
  //   def s(x : Unit) : (Unit, Unit) = ((), ())

  //   val tree = rootTree(())
  //   val splitTree = tree.splitLeaf(rootLabel, s)
  //   assert(!splitTree.hasLeaf(rootLabel))
  //   assert(splitTree.hasLeaf(rootLabel.left))
  //   assert(splitTree.hasLeaf(rootLabel.right))
  // }

  // "depth first traversal" should "contain exactly ancestors exactly once" in {
  //   def s(x : Unit) : (Unit, Unit) = ((), ())

  //   val tree = rootTree(()).
  //     splitLeaf(rootLabel, s).
  //     splitLeaf(rootLabel.left, s).
  //     splitLeaf(rootLabel.left.left, s).
  //     splitLeaf(rootLabel.left.left.left, s).
  //     splitLeaf(rootLabel.left.left.right, s).
  //     splitLeaf(rootLabel.left.right, s).
  //     splitLeaf(rootLabel.right, s).
  //     splitLeaf(rootLabel.right.left, s).
  //     splitLeaf(rootLabel.right.left.right, s).
  //     splitLeaf(rootLabel.right.right, s).
  //     splitLeaf(rootLabel.right.right.left, s)

  //   val leaves = tree.leafNodes
  //   val ancestors1 = leaves.flatMap(x => x.ancestors.toSet)
  //   val dft = tree.dftInternal
  //   val ancestors2 = dft.toSet

  //   assert(ancestors1 === ancestors2)
  //   assert(dft.toSet.size === dft.length)
  // }

  // "PCFunction" should "retain root box after split and merge" in {

  //   def s(x : Unit) : (Unit, Unit) = ((), ())

  //   val pcf1 = constantPCFunction(bb, ())
  //   val pcf2 = pcf1.splitCell(rootLabel, { _ => ((), ()) }).mergeCell(rootLabel, { (_, _) => () })

  //   equal(pcf1, pcf2)
  // }

  // it should "halv volume at each split" in {
  //   def s(x : Unit) : (Unit, Unit) = ((), ())

  //   var f = constantPCFunction(bb, ())
  //   val (lab1, box1, _) = f(rootLabel)
  //   f = f.splitCell(rootLabel, s)
  //   val (lab2, box2, _) = f(rootLabel.right)
  //   f = f.splitCell(rootLabel.right, s)
  //   val (lab3, box3, _) = f(rootLabel.right.right)
  //   assert(bb.volume / pow(2, lab1.depth) === box1.volume)
  //   assert(bb.volume / pow(2, lab2.depth) === box2.volume)
  //   assert(bb.volume / pow(2, lab3.depth) === box3.volume)
  // }

  // "density" should "should not depend on partition" in {
  //   val h1 = Histogram(100, bb.volume, constantPCFunction(bb, 100L))
  //   val h2 = Histogram(100, bb.volume, constantPCFunction(bb, 100L).
  //                        splitCell(rootLabel, _ => (50L, 50L)))
  //   assert(h1.density(rootLabel) === h2.density(rootLabel.left))
  //   assert(h1.density(rootLabel) === h2.density(rootLabel.right))
  // }

  // "likelihood" should "should not depend on partition" in {
  //   val h1 = Histogram(100, bb.volume, constantPCFunction(bb, 100L))
  //   val h2 = Histogram(100, bb.volume, constantPCFunction(bb, 100L).
  //                        splitCell(rootLabel, _ => (50L, 50L)))
  //   assert(h1.logLik() === h2.logLik())
  // }

  // "LOO-error approx" should "should not depend on partition" in {
  //   val h1 = Histogram(100, bb.volume, constantPCFunction(bb, 100L))
  //   val h2 = Histogram(100, bb.volume, constantPCFunction(bb, 100L).
  //                        splitCell(rootLabel, _ => (50L, 50L)))
  //   assert(h1.looL2ErrorApprox() === h2.looL2ErrorApprox())
  // }
}
