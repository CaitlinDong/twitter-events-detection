import sbt._

object Dependencies {
  import Versions._

  lazy val main = Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-streaming" % sparkV,
    "org.apache.spark" %% "spark-streaming-twitter" % sparkV,
    "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2"
   // "org.twitter4j" % "twitter4j-core" % "4.0.4"
  )
}