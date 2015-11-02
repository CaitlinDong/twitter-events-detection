package ted

import akka.io.Udp.SO.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Level, Logger}
import ted.StreamingExamples
import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf._
import edu.stanford.nlp.io.IOUtils
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.sequences.DocumentReaderAndWriter
import edu.stanford.nlp.util.Triple
import java.util.List


/**
 * Twitter Events Detection -
 * Clusters tweets from the Twitter Stream based on location information extracted from the tweet text.
 * The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 */
object ServiceBootstrap {

  def extractLocation(x:String,serializedClassifier: String): String = {
    val classifier: AbstractSequenceClassifier[CoreLabel] = CRFClassifier.getClassifier(serializedClassifier)
    classifier.classifyWithInlineXML(x)
  }


  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    //Setting Streaming Log Levels
    StreamingExamples.setStreamingLogLevels()
    //Logger.getRootLogger.setLevel(Level.WARN)

    //Setting up Spark Context
    val sparkConf = new SparkConf().setAppName("ServiceBootstrap")
    val sc = new SparkContext(sparkConf);

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    //Setup Spark Streaming Context
    val ssc = new StreamingContext(sc, Seconds(2))

    //Create stream from twitter
    val stream = TwitterUtils.createStream(ssc, None, filters)

    //Take in location dataset
    //val locationFile = sc.textFile("/Users/namitsharma/Dropbox/Projects/spark-1.5.0-bin-hadoop2.6/worldcitiespop.txt");

    //Split a record into an Array(Country, City, AccentCity, Region, Population, Latitude, Longitude)
    //val lines = locationFile.map(line => line.split(",")).cache();

    //Just take cities
    //val cities = lines.map(line=>line(1)).collect();
    //val broadcastVar = sc.broadcast(cities);

    //cities.foreach({t=>println(t)});

    //Extract Location Data to get a stream like (tweet,location) where location is ideally found from tweetText
    //val stream2 = stream.map(x => (x,extractLocation(x.getText(),broadcastVar.value)));
    //Make some comparison with the location found and the location of the tweeter
    //Also handle the case when location is not found
    //val stream2=stream.map(x=>(x.getText(),"yoyoyoyolocation"));

    /*
        //Clustering on a window
        val myWindowedStream = stream.window(Seconds(30), Seconds(10))
        myWindowedStream.foreachRDD(y => {
          println(y.map(x => x.getText().length()).reduce((a, b) => a + b))
        })
    */
    stream.map(x=>x.getText()).saveAsTextFiles("./src/main/tweets/earthquake_tweets");

    var serializedClassifier: String = "src/main/resources/classifiers/english.all.3class.distsim.crf.ser.gz"
    //val classifier: AbstractSequenceClassifier[CoreLabel] = CRFClassifier.getClassifier(serializedClassifier)
    //val broadcastVar = sc.broadcast(classifier);

    //broadcastVar.value

    //Simply printing it
    stream.foreachRDD(rdd => {
      println("\nNumber of Tweets in 2 second batches is (%s)".format(rdd.count()))
      val tweetTextArray = rdd.map(x => (x.getText(),extractLocation(x.getText(),serializedClassifier)));
      tweetTextArray.foreach{t => println(t)}

    })

    ssc.start()
    ssc.awaitTermination()
  }
}
