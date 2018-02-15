name := "Distributed Density Thingie"

version := "0.0.1"

scalaVersion in ThisBuild := "2.11.8"
scalaVersion := "2.11.8"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

fork in test := true
scalacOptions := Seq("-unchecked", "-deprecation", "-feature")

// For Spark tests
parallelExecution in test := false

// libraryDependencies += "co.theasi" %% "plotly" % "0.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0"
// libraryDependencies += "com.github.wookietreiber" %% "scala-chart" % "latest.integration"
libraryDependencies += "org.vegas-viz" %% "vegas" % "0.3.11"
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.16"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
