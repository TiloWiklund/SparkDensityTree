name := "Distributed Density Thingie"

version := "0.0.1"

scalaVersion in ThisBuild := "2.11.8"
scalaVersion := "2.11.8"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0"
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.16"
