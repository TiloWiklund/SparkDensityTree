import scala.language.postfixOps

import org.scalatest._

import ScalaDensity._
import org.apache.spark.mllib.linalg.{ Vector => MLVector, _ }
import scala.math.{abs}

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

  private val dfnum = 200
  private val dfdim = 3

  override protected def beforeAll() : Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("ScalaDensityTest").setMaster("local")
    sc = new SparkContext(conf)
    df = normalVectorRDD(sc, dfnum, dfdim).persist()
  }

  override protected def afterAll() : Unit = {
    df.unpersist()
    sc.stop()
  }

  "Rectangle" should "preserve total volume when split" in {
    val box = boundingBox(df)

    for(i <- 0 until dfdim)
      assert(abs(box.volume-box.lower(i).volume-box.upper(i).volume) < 0.0000001)
  }

  it should "split into equal volumes" in {
    val box = boundingBox(df)

    for(i <- 0 until dfdim)
      assert(abs(box.lower(i).volume - box.upper(i).volume) < 0.0000001)
  }

  it should "have expected dimension" in {
    val box = boundingBox(df)

    assert(box.dim === dfdim)
  }

  "single point" should "have volume 0" in {
    assert(point(df.first()).volume === 0)
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

  "rootTree" should "have root node as leaf" in {
    val tree = rootTree(())
    assert(tree.hasLeaf(rootLabel))
  }

  it should "replace root node after split" in {
    def s(x : Unit) : (Unit, Unit) = ((), ())

    val tree = rootTree(())
    val splitTree = tree.splitLeaf(rootLabel, s)
    assert(!splitTree.hasLeaf(rootLabel))
    assert(splitTree.hasLeaf(rootLabel.left))
    assert(splitTree.hasLeaf(rootLabel.right))
  }

  "depth first traversal" should "should contain exactly ancestors exactly once" in {
    def s(x : Unit) : (Unit, Unit) = ((), ())

    val tree = rootTree(()).
      splitLeaf(rootLabel, s).
      splitLeaf(rootLabel.left, s).
      splitLeaf(rootLabel.left.left, s).
      splitLeaf(rootLabel.left.left.left, s).
      splitLeaf(rootLabel.left.left.right, s).
      splitLeaf(rootLabel.left.right, s).
      splitLeaf(rootLabel.right, s).
      splitLeaf(rootLabel.right.left, s).
      splitLeaf(rootLabel.right.left.right, s).
      splitLeaf(rootLabel.right.right, s).
      splitLeaf(rootLabel.right.right.left, s)

    val leaves = tree.leafNodes
    val ancestors1 = leaves.flatMap(x => x.ancestors.toSet)
    val dft = tree.dftInternal
    val ancestors2 = dft.toSet

    assert(ancestors1 === ancestors2)
    assert(dft.toSet.size === dft.length)
  }

  "PCFunction" should "retain root box after split and merge" in {
    val pcf1 = constantPCFunction(boundingBox(df), ())
    val pcf2 = pcf1.splitCell(rootLabel, { _ => ((), ()) }).mergeCell(rootLabel, { (_, _) => () })

    equal(pcf1, pcf2)
  }
}
