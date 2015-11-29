package ted

import akka.io.Udp.SO.Broadcast
import edu.stanford.nlp.parser.nndep.Classifier
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Level, Logger}
import ted.StreamingExamples

import scala.collection.mutable.ListBuffer

/* For Stanford NLP */
import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf._
import edu.stanford.nlp.io.IOUtils
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.sequences.DocumentReaderAndWriter
import edu.stanford.nlp.util.Triple



import scala.collection.immutable.List


/* For extracting Location from XML */
import org.apache.commons.lang.StringUtils

/* For Geocode Tagging */
import java.net.URLConnection
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.io.InputStreamReader
import java.io.BufferedReader
import javax.xml.parsers.DocumentBuilder
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPath
import javax.xml.xpath.XPathExpression
import javax.xml.xpath.XPathFactory
import javax.xml.xpath.XPathConstants
import org.w3c.dom.Document


/**
 * Twitter Events Detection -
 * Clusters tweets from the Twitter Stream based on location information extracted from the tweet text.
 * The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 */
object ServiceBootstrap {
/*
  def extractLocationCoordinates(tweet_with_NER:String): String = {

    if (tweet_with_NER.contains("<LOCATION>")) {
      val locations_array = StringUtils.substringsBetween(tweet_with_NER, "<LOCATION>", "</LOCATION>")
      val locations_string = locations_array(0);
      //val lat_long_array = locations_array.map(x=>getLatLongPositions(x).mkString(" "))
      //val lat_long_string = lat_long_array.mkString(",")
      return locations_string;//+","+lat_long_string
    }
    return ""
  }
  */

  def splitForEachLocation(tweetTextWithLocations : List[String]): List[List[String]] = {
    val splitBuffer: ListBuffer[List[String]] = new ListBuffer()

    for (i <- 1 to tweetTextWithLocations.length-1)
      splitBuffer.append(List(tweetTextWithLocations(i), tweetTextWithLocations(0)))
    splitBuffer.toList
  }

  def extractLocations(tweetText: String): List[String] = {
    if (tweetText.contains("<LOCATION>"))
      StringUtils.substringsBetween(tweetText, "<LOCATION>", "</LOCATION>").toList.map(_.toLowerCase)
    else List("No Location")
  }

  @throws(classOf[Exception])
  def getLatLongPositions(address: String): Array[String] = {
    var responseCode: Int = 0
    val api: String = "http://maps.googleapis.com/maps/api/geocode/xml?address=" + URLEncoder.encode(address, "UTF-8") + "&sensor=true"
    val url: URL = new URL(api)
    val httpConnection: HttpURLConnection = url.openConnection.asInstanceOf[HttpURLConnection]
    httpConnection.connect
    responseCode = httpConnection.getResponseCode
    if (responseCode == 200) {
      val builder: DocumentBuilder = DocumentBuilderFactory.newInstance.newDocumentBuilder

      val document: Document = builder.parse(httpConnection.getInputStream)
      val xPathfactory: XPathFactory = XPathFactory.newInstance
      val xpath: XPath = xPathfactory.newXPath
      var expr: XPathExpression = xpath.compile("/GeocodeResponse/status")
      val status: String = expr.evaluate(document, XPathConstants.STRING).asInstanceOf[String]
      if (status == "OK") {
        expr = xpath.compile("//geometry/location/lat")
        val latitude: String = expr.evaluate(document, XPathConstants.STRING).asInstanceOf[String]
        expr = xpath.compile("//geometry/location/lng")
        val longitude: String = expr.evaluate(document, XPathConstants.STRING).asInstanceOf[String]
        return Array[String](latitude, longitude)
      }
      else {
        throw new Exception("Error from the API - response status: " + status)
      }
    }
    return null
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
    //val stream = TwitterUtils.createStream(ssc, None, filters)
    val stream = ssc.socketTextStream("localhost", 9999)

    /* Uncomment if you wanna start saving the tweets */
    //stream.map(x=>x.getText()).saveAsTextFiles("./src/main/tweets/earthquake_tweets");

    //Declaring lazy NERClassifier
    var serializedClassifier: String = "src/main/resources/classifiers/english.all.3class.distsim.crf.ser.gz"
    object NER {
      @transient lazy val classifier: AbstractSequenceClassifier[CoreLabel] = CRFClassifier.getClassifier(serializedClassifier)
    }

    /* Code for Location Extraction Goes here
       To Extract Location Data We use Stanford's NER
     */

    //List(Tweet,Location1,Location2...)
    val stream2 = stream.map(x => x :: extractLocations(NER.classifier.classifyWithInlineXML(x)))
    //With Twitter:
    //val stream2 = stream.map(x => x :: extractLocations(NER.classifier.classifyWithInlineXML(x.getText))).map(x => x if ...)

    /*stream2.foreachRDD(rdd => {
      println("Tweets extracted from 2 second batches (%s)".format(rdd.count()))
      rdd.foreach{t => println(t(0))}
    })*/

    //List(Location,Tweet)
    val stream3 = stream2.flatMap(t => splitForEachLocation(t))
    /*
    stream3.foreachRDD(rdd => {
      println("Locations with Tweets extracted from 2 second batches (%s)".format(rdd.count()))
      rdd.foreach{t => println(t)}
    })*/



    //Code for Clustering on a window goes here
    val myWindowedStream = stream3.map(x => (x(0),x(1))).window(Seconds(30), Seconds(10))
    myWindowedStream.foreachRDD(rdd => {
      println("\nGrouping by location (%s)".format(rdd.count()))
      //(Location,CompactBuffer())
      val new_rdd = rdd.groupByKey().map(x=>(x._2.size,(x,getLatLongPositions(x._1).toList))).sortByKey(false)
      new_rdd.foreach{t => println(t)}
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
