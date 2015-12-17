import sbt._
import Keys._
import Versions._

lazy val rootSettings: Seq[Setting[_]] = Seq(
  organization:= "edu.ncsu",
  version:= "1.0",
  scalaVersion := scalaV,

  scalacOptions in Compile ++= Seq(
    "-unchecked",
    "-feature",
    "-language:postfixOps",
    "-deprecation",
    "-encoding",
    "utf8"
  )
)

lazy val projSettings = rootSettings ++ Seq(
  name := "twitter-events-detection",
  libraryDependencies := Dependencies.main,
  assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  assemblyJarName in assembly := "twitter-events-detection.jar",
  mainClass in assembly := Some("ted.ServiceBootstrap")
)

lazy val root = Project("twitter-events-detection", file("."))
  .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*) // required by 'sbt-dependency-graph'
  .settings(projSettings: _*)