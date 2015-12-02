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
import twitter4j.json.DataObjectFactory

import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer

/* For Stanford NLP */
import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf._
import edu.stanford.nlp.io.IOUtils
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.sequences.DocumentReaderAndWriter
import edu.stanford.nlp.util.Triple

/* For extracting Location from XML */
import org.apache.commons.lang.StringUtils

/* For Geocode Tagging */
import java.net.URLConnection
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.io.{PrintStream, InputStreamReader, BufferedReader}
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

  def splitForEachLocation(tweet: twitter4j.Status, locations : List[String]): List[(String,twitter4j.Status)] = {
    val splitBuffer: ListBuffer[(String,twitter4j.Status) ] = new ListBuffer()

    for (i <- 0 to locations.length-1)
      splitBuffer.append((locations(i),tweet))
    splitBuffer.toList
  }

  def extractLocations(tweetText: String): List[String] = {
    if (tweetText.contains("<LOCATION>"))
      StringUtils.substringsBetween(tweetText, "<LOCATION>", "</LOCATION>").toList.map(_.toLowerCase)
    else List("No Location")
  }

  //@throws(classOf[Exception])
  def getLatLongPositions(address: String): List[String] = {
    if(address equals("someother location"))
      {
        return List()
      }
    var responseCode: Int = 0
    //val api: String = "http://www.datasciencetoolkit.org/maps/api/geocode/json?sensor=false&address=" + URLEncoder.encode(address, "UTF-8")
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
        List(latitude, longitude)
      }
      else {
        //throw new Exception("Error from the API - response status: " + status)
        List("NoCoords")
      }
    }
    else List()
  }

  def main(args: Array[String]) {

    val myCommandlineParameters = Array("63jvU9P2FaJLcqh704yZ2rPWs","tEJwsJNPNvckcQXqJEQDNVjvE8hvitEjV6is5OvqCh1DYZLiAo","98218547-l3RyOPgaGthTDRQJxNAy2eMigpdNtNxgJxJoc9o0j","RoOTzJyMxps7meXV0cUiTjLJPuCY3GDA5rqbkFc2GmKsR","earthquake","tremor","quake","richter")
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = myCommandlineParameters.take(4)
    val filters = myCommandlineParameters.takeRight(myCommandlineParameters.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    //Setting Streaming Log Levels
    StreamingExamples.setStreamingLogLevels()

    //Setting up Spark Context
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ServiceBootstrap")
    val sc = new SparkContext(sparkConf)

    //Setup Spark Streaming Context
    val ssc = new StreamingContext(sc, Seconds(2))

    //Create stream from twitter
    //val stream = TwitterUtils.createStream(ssc, None, filters)

    //Take Twitter json data from socket
    val stream = ssc.socketTextStream("localhost", 9998).map(x => DataObjectFactory.createObject(x).asInstanceOf[twitter4j.Status])

    /* Uncomment if you wanna start saving the tweets */
    //stream.saveAsTextFiles("./src/main/tweets/earthquake_tweets");

    //Declaring lazy NERClassifier
    var serializedClassifier: String = "src/main/resources/classifiers/english.all.3class.distsim.crf.ser.gz"
    object NER {
      @transient lazy val classifier: AbstractSequenceClassifier[CoreLabel] = CRFClassifier.getClassifier(serializedClassifier)
    }

    //Location Extraction using Stanford's NER
    val stream2 = stream
      .map(x => (x,extractLocations(NER.classifier.classifyWithInlineXML(x.getText))))//(twitter4j.Status,List[LocationString])
      .map(x => {
        if (x._2.head equals "No Location"){
          (x._1,List("someother location"))
        }
        else x
      })

    //Todo
    //Remove commas     //s.map(c => if(c == '0') ' ' else c)
    //Capitalize all first letter of all words

    //Setup a locationsHaspMap RDD
    //val locationsHaspMap = sc.textFile("src/main/resources/coordinates");

    //Split record by location
    val stream3 = stream2.flatMap(t => splitForEachLocation(t._1,t._2))//(LocationString,twitter4j.Status)

    //Code for Clustering on a window goes here
    val myActivityStream = stream3
      .map(x => (x._1,(x._2.getText,getLatLongPositions(x._1))))//(LocationString,(twitter4j.Status.getText,List(LatitudeString,LongitudeString)))
      .window(Seconds(12*60*60), Seconds(10))
      .groupByKey() //(Location,Iterable<(twitter4j.Status.getText,List(LatitudeString,LongitudeString))>)

    myActivityStream.foreachRDD(rdd => {
      println("\nMy Activity (%s)".format(rdd.count()))

      val new_rdd = rdd.map(x => (x._2.size,(x._1,x._2.head._2))).filter(_._1>=3).sortByKey(false) //(TweetCountByLocation,(Location,List(LatitudeString,LongitudeString))

      new_rdd.foreach{t => println(t)}
      new_rdd.saveAsTextFile("./src/main/grouped_tweets/earthquake_tweets");
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
