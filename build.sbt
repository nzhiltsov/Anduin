import AssemblyKeys._

name := "anduin"

version := "0.3.1"

organization := "ru.ksu.niimm.cll"

scalaVersion := "2.10.3"

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/content/repositories/releases",
  "Twitter Maven" at "http://maven.twttr.com",
  "Twitter SVN Maven" at "https://svn.twitter.biz/maven-public",
  "Clojars Repository" at "http://clojars.org/repo"
)

libraryDependencies += "com.twitter" % "scalding_2.10" % "0.11.0"

libraryDependencies += "com.twitter" % "scalding-commons_2.10" % "0.11.0"

libraryDependencies += "org.scala-tools.testing" % "specs_2.10" % "1.6.9" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.0" % "test"

libraryDependencies += "junit" % "junit" % "4.8" % "test"

libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "4.3"

libraryDependencies += "org.apache.commons" % "commons-compress" % "1.4.1"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.1"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.9"

libraryDependencies += "org.jsoup" % "jsoup" % "1.7.2"

parallelExecution in Test := false

seq(assemblySettings: _*)

mainClass in(Compile, run) := Some("com.twitter.scalding.Tool")

excludedJars in assembly <<= (fullClasspath in assembly) map {
  cp => cp filter {
    Set("janino-2.5.16.jar", "hadoop-core-0.20.2.jar", "jsp-api-2.1-6.1.14.jar", "jsp-2.1-6.1.14.jar",
      "jasper-compiler-5.5.12.jar") contains _.data.getName
  }
}

mergeStrategy in assembly <<= (mergeStrategy in assembly) {
  (old) => {
    case s if s.endsWith(".class") => MergeStrategy.last
    case s if s.endsWith("project.clj") => MergeStrategy.concat
    case s if s.endsWith(".html") => MergeStrategy.last
    case x => old(x)
  }
}

net.virtualvoid.sbt.graph.Plugin.graphSettings
