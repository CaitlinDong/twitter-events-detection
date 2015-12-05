import sbt._

object Dependencies {
  import Versions._

  lazy val main = Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-streaming" % sparkV,
    "org.apache.spark" %% "spark-streaming-twitter" % sparkV,
    "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2",
    "io.spray" % "spray-json_2.11" % "1.3.2"

    //"com.typesafe.play" % "play-ws_2.11" % "2.5.0-M1"
    // "org.twitter4j" % "twitter4j-core" % "4.0.4"
  )
}