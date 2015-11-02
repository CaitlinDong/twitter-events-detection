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
  ),

  resolvers ++= AdditionalResolvers.resolvers
)

lazy val projSettings = rootSettings ++ Seq(
  name := "twitter-events-detection",
  libraryDependencies := Dependencies.main
)

lazy val root = Project("twitter-events-detection", file("."))
  .settings(projSettings: _*)

