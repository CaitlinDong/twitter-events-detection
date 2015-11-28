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

/* For Stanford NLP */
import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf._
import edu.stanford.nlp.io.IOUtils
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.sequences.DocumentReaderAndWriter
import edu.stanford.nlp.util.Triple
import java.util.List



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

  def extractLocationCoordinates(tweet_with_NER:String): String = {

    if (tweet_with_NER.contains("<LOCATION>")) {
      val locations_array = StringUtils.substringsBetween(tweet_with_NER, "<LOCATION>", "</LOCATION>")
      val locations_string = locations_array.mkString(",");
      val lat_long_array = locations_array.map(x=>getLatLongPositions(x).mkString(" "))
      val lat_long_string = lat_long_array.mkString(",")
      return locations_string+","+lat_long_string
    }
    return ""
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
    val stream = TwitterUtils.createStream(ssc, None, filters)
    //val stream = ssc.textFileStream("./src/main/tweets/")

   // val stream = ssc.textFileStream("./src/main/tweets/")

    //val stream = ssc.socketTextStream("localhost", 7777)


    /* Uncomment if you wanna start saving the tweets */
    //stream.map(x=>x.getText()).saveAsTextFiles("./src/main/tweets/earthquake_tweets");


    /*
      Code to use the location Dataset
     */
    //val locationFile = sc.textFile("/Users/namitsharma/Dropbox/Projects/spark-1.5.0-bin-hadoop2.6/worldcitiespop.txt");
    //import sqlContext._
    //import sqlContext.implicits._
    //case class Locations(Country :String , City:String, AccentCity: String, Region: String, Population:String, Latitude: String, Longitude:String)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //val locations_df = sc.textFile("/Users/namitsharma/Dropbox/Projects/spark-1.5.0-bin-hadoop2.6/worldcitiespop.txt").map(_.split(",")).map(p => Locations(p(0),p(1),p(2),p(3),p(4),p(5),p(6))).toDF()
    //val teenagers = sqlContext.sql("SELECT Country,Lat,Long FROM people WHERE age >= 13 AND age <= 19")
    //locations_df.registerTempTable("mylocations")

    //val c = sqlContext.sql("SELECT count(*) FROM mylocations")


    //Split a record into an Array(Country, City, AccentCity, Region, Population, Latitude, Longitude)
    //val lines = locationFile.map(line => line.split(",")).cache();

    //Just take cities
    //val cities = lines.map(line=>line(1)).collect();
    //val broadcastVar = sc.broadcast(cities);
    //cities.foreach({t=>println(t)});



    /* Code for Location Extraction Goes here
       Extract Location Data to get a stream like (tweet,location) where location is ideally found from tweetText
       We first use Stanford's NER to get location and then get Coordinates from the location.
     */

    var serializedClassifier: String = "src/main/resources/classifiers/english.all.3class.distsim.crf.ser.gz"
    object NER {
      @transient lazy val classifier: AbstractSequenceClassifier[CoreLabel] = CRFClassifier.getClassifier(serializedClassifier)
    }
    //val broadcastVar = sc.broadcast(classifier);

    //Simply printing it
    //It will be printed like this : (tweet text,Location1,Location2...,Lat1 Long1,Lat2 Long2...)
   //example - (A 3.18 magnitude earthquake has occurred near Nukuhou, Bay Of Plenty, New Zealand on 11/9/15, 1:35 AM! https://t.co/WIh9CEPyS3,Nukuhou,Bay Of Plenty,New Zealand,-38.1250730 177.1295343,-37.6825027 176.1880232,-40.9005570 174.8859710)

    stream.foreachRDD(rdd => {
      println("\nNumber of Tweets in 2 second batches is (%s)".format(rdd.count()))
      val tweetTextArray = rdd.map(x => (x.getText,extractLocationCoordinates(NER.classifier.classifyWithInlineXML(x.getText))));
      //val tweetTextArray = rdd.map(x=>NER.classifier.classifyWithInlineXML(x.getText));
      tweetTextArray.foreach{t => println(t)}

    })


    /*
       //Code for Clustering on a window goes here

       val myWindowedStream = stream.window(Seconds(30), Seconds(10))
       myWindowedStream.foreachRDD(y => {
         println(y.map(x => x.getText().length()).reduce((a, b) => a + b))
       })


   */


    ssc.start()
    ssc.awaitTermination()
  }
}
