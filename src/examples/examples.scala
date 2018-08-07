import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{ Logger, Level }

import java.io.{File, PrintWriter}
import org.apache.spark.mllib.random.RandomRDDs.normalVectorRDD


object Examples {
  def main(args: Vector[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("ScalaDensity").setMaster("local[3]")//setMaster("spark://127.0.0.1:7077")
    val sc = new SparkContext(conf)

    val dfnum = 2000
    val dfdim = 3
    // val df = normalVectorRDD(sc, n, 2)
    val df = normalVectorRDD(sc, dfnum, dfdim, 3, 7387389)

    def supportCarveLim(totalVolume : Double, totalCount : Count)(depth : Int, volume : Volume, count : Count) =
      count > dfnum/2 || (1 - count/totalCount)*volume/totalVolume > 0.01

    def countLim(totalVolume : Double, totalCount : Count)(depth : Int, volume : Volume, count : Count) =
      count > 10

    // factor out totalCount and totalVolume since they are the same for all nodes
    def supportCarvePriority(lab : NodeLabel, c : Count, v : Volume) : Double = (1 - c)*v
    def countPriority(lab : NodeLabel, c : Count, v : Volume) : Count = c

    val supportCarvedH = histogram(df, supportCarveLim, noEarlyStop)

    val temp = 0.005

    var maxH = supportCarvedH
    var maxLik = supportCarvedH.logPenalisedLik(temp)

    supportCarvedH.backtrack(supportCarvePriority).foreach {
      case h =>
        val hLik = h.logPenalisedLik(1/temp)
        if(hLik > maxLik) {
          maxH = h
          maxLik = hLik
        }
    }

    val tributaryH = histogramStartingWith(maxH, df, countLim, noEarlyStop)

    var minH = tributaryH
    var minLoo = minH.looL2ErrorApprox
    tributaryH.backtrack(countPriority).foreach {
      case h =>
        val hLoo = h.looL2ErrorApprox
        if(hLoo < minLoo) {
          minH = h
          minLoo = hLoo
        }
    }

    println("LOO scores:")
    print("Support carved end: ")
    println(supportCarvedH.looL2ErrorApprox)
    print("Support carved maxLik: ")
    println(maxH.looL2ErrorApprox)
    print("Tributary end: ")
    println(tributaryH.looL2ErrorApprox)
    print("Tributary minLoo: ")
    println(minH.looL2ErrorApprox)

    sc.stop()
  }

  def testRun() : Histogram = {
    val dfnum = 5000
    val dfdim = 3
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("ScalaDensityTest").setMaster("local")
    val sc = new SparkContext(conf)
    val df = normalVectorRDD(sc, dfnum, dfdim, 6, 7387389).cache()
    def lims(totalVolume : Double, totalCount : Count)(depth : Int, volume : Volume, count : Count) =
      count > dfnum/2 || (1 - count/totalCount)*volume > 0.01*totalVolume
    histogram(df, lims, noEarlyStop)
  }

  def raazRun() = {
    val h = testRun()

    val bbF = new PrintWriter(new File("/home/tilo/Project/Research/distributed-histogram-trees/raaz_thing/rootBox.txt"))
    try {
      for( (l, h) <- h.tree.rootCell.factorise() ) {
        bbF.write(s"[$l,$h]\n")
      }
    } finally {
      bbF.close()
    }

    val smallnodeF = new PrintWriter(new File("/home/tilo/Project/Research/distributed-histogram-trees/raaz_thing/smallnodes.txt"))
    try {
      for(lab <- h.counts.truncation.leaves) {
        smallnodeF.write(f"${lab.lab}%d\n")
      }
    } finally {
      smallnodeF.close()
    }

    val denseF = new PrintWriter(new File("/home/tilo/Project/Research/distributed-histogram-trees/raaz_thing/ranges.txt"))
    val depthF = new PrintWriter(new File("/home/tilo/Project/Research/distributed-histogram-trees/raaz_thing/ldfn.txt"))
    val nodeF  = new PrintWriter(new File("/home/tilo/Project/Research/distributed-histogram-trees/raaz_thing/nodes.txt"))
    try {
      for((l,maybec) <- h.counts.minimalCompletionNodes()) {
        depthF.write(f"${l.depth}%d\n")
        nodeF.write(f"${l.lab}%d\n")
        maybec match {
          case Some(c) => denseF.write(f"${c / (h.totalCount * h.tree.volumeAt(l))}%2.2f\n")
          case None    => denseF.write(f"${0}%2.2f\n")
        }
      }
    } finally {
      denseF.close()
      depthF.close()
      nodeF.close()
    }
  }
}

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{ Logger, Level }

import java.io.{File, PrintWriter}
