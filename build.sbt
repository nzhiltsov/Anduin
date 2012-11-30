name := "anduin"

version := "0.1"

organization := "ru.ksu.niimm.cll"

scalaVersion := "2.9.2"

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "http://oss.sonatype.org/content/repositories/releases"
)

libraryDependencies += "com.twitter" % "scalding_2.9.2" % "0.8.1" withSources()

libraryDependencies += "org.scala-tools.testing" % "specs_2.9.2" % "1.6.9" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.8" % "test"

libraryDependencies += "junit" % "junit" % "4.8" % "test"
