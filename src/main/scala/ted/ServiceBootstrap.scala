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
import scala.util.parsing.json.JSONObject

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



import spray.json._
import DefaultJsonProtocol._
import scala.io.Source.fromURL

/**
 * Twitter Events Detection -
 * Clusters tweets from the Twitter Stream based on location information extracted from the tweet text.
 * The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 */

object ServiceBootstrap {

  def splitForEachLocation(tweet: twitter4j.Status, locations: List[String]): List[(String, twitter4j.Status)] = {
    val splitBuffer: ListBuffer[(String, twitter4j.Status)] = new ListBuffer()

    for (i <- 0 to locations.length - 1)
      splitBuffer.append((locations(i), tweet))
    splitBuffer.toList
  }

  def extractLocations(tweetText: String): List[String] = {
    val tweetTextCapitalised = tweetText.split(" ").map(x=>x.capitalize).mkString
    if (tweetTextCapitalised.contains("<LOCATION>"))
      StringUtils.substringsBetween(tweetText, "<LOCATION>", "</LOCATION>").toList.map(_.toLowerCase)
    else List("No Location")
  }

  //@throws(classOf[Exception])
  def getLatLongFromGoogleGeocode(address: String): List[String] = {
    if(address equals("someother location"))
      {
        return List()
      }
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
        List(latitude, longitude)
      }
      else {
        //throw new Exception("Error from the API - response status: " + status)
        List("NoCoords")
      }
    }
    else List()
  }

  def getLatLongFromDSTKServer(address: String): List[String] = {

    if(address equals("someother location"))
    {
      return List()
    }
    case class GeoCodeLongLat(lng: Double, lat: Double)
    case class GeoCodeViewport(northeast: GeoCodeLongLat, southwest: GeoCodeLongLat)
    case class GeoCodeAddressComponent(long_name: String, types: Array[String], short_name: String)
    case class GeoCodeGeometry(viewport: GeoCodeViewport, location_type: String, location: GeoCodeLongLat)
    case class GeoCodeResult(formatted_address: String, geometry: GeoCodeGeometry, types: Array[String], address_components: Array[GeoCodeAddressComponent])
    case class GeoCodeDocument(results: Array[GeoCodeResult], status: String)

    implicit val GeoCodeLongLatJF = jsonFormat2(GeoCodeLongLat)
    implicit val GeoCodeViewportJF = jsonFormat2(GeoCodeViewport)
    implicit val GeoCodeAddressComponentJF = jsonFormat3(GeoCodeAddressComponent)
    implicit val GeoCodeGeometryJF = jsonFormat3(GeoCodeGeometry)
    implicit val GeoCodeResultJF = jsonFormat4(GeoCodeResult)
    implicit val GeoCodeDocumentJF = jsonFormat2(GeoCodeDocument)


    var responseCode: Int = 0
    val api: String = "http://ec2-52-23-192-139.compute-1.amazonaws.com/maps/api/geocode/json?address=" + URLEncoder.encode(address, "UTF-8")
    val url: URL = new URL(api)
    val httpConnection: HttpURLConnection = url.openConnection.asInstanceOf[HttpURLConnection]
    httpConnection.connect
    responseCode = httpConnection.getResponseCode
    if (responseCode == 200) {
      val in: BufferedReader = new BufferedReader(new InputStreamReader(httpConnection.getInputStream))
      var inputLine: String = null
      val tmp: StringBuffer = new StringBuffer

      while ({
        inputLine = in.readLine; inputLine
      } != null) {
        tmp.append(inputLine)
      }
      in.close

      val response = tmp.toString

      println(response)

      val geocode = response.parseJson.convertTo[GeoCodeDocument]

      val status = geocode.status
      if (status == "OK") {
        val result = geocode.results.toList.head.geometry.location
        List(result.lat.toString, result.lng.toString)
      }
      else {
        //throw new Exception("Error from the API - response status: " + status)
        List("NoCoords")
      }
    }
    else List(responseCode.toString)
  }


  def getLatLongFromTwoFishes(address: String): List[String] = {

    if(address equals("someother location"))
    {
      return List()
    }
    case class Center(
                       lat: Double,
                       lng: Double
                       )
    case class Bounds(
                       ne: Center,
                       sw: Center
                       )
    case class Geometry(
                         center: Center,
                         bounds: Option[Bounds],
                         source: Option[String]
                         )
    case class Ids(
                    source: String,
                    id: String
                    )
    case class Names(
                      name: String,
                      lang: String,
                      flags: List[Double]
                      )
    case class Attributes(
                           adm0cap: Option[Double],
                           scalerank: Option[Double],
                           labelrank: Option[Double],
                           natscale: Option[Double],
                           population: Option[Double],
                           urls: Option[List[String]],
                           worldcity: Option[Double]
                           )
    case class Feature(
                        cc: Option[String],
                        geometry: Geometry,
                        name: Option[String],
                        displayName: Option[String],
                        woeType: Option[Double],
                        ids: Option[List[Ids]],
                        names: Option[List[Names]],
                        highlightedName: Option[String],
                        matchedName: Option[String],
                        id: Option[String],
                        attributes: Option[Attributes],
                        longId: Option[String],
                        longIds: Option[List[String]],
                        parentIds: Option[List[String]]
                        )
    case class Interpretations(
                                what: String,
                                where: String,
                                feature: Feature
                                )
    case class TwoFishesJsonObject(
                               interpretations: List[Interpretations]
                               )


    implicit val CenterJF = jsonFormat2(Center)
    implicit val BoundsJF = jsonFormat2(Bounds)
    implicit val GeometryJF = jsonFormat3(Geometry)
    implicit val IdsJF = jsonFormat2(Ids)
    implicit val NamesJF = jsonFormat3(Names)
    implicit val AttributesJF = jsonFormat7(Attributes)
    implicit val FeatureJF = jsonFormat14(Feature)
    implicit val InterpretationsJF = jsonFormat3(Interpretations)
    implicit val TwoFishesJsonObjectJF = jsonFormat1(TwoFishesJsonObject)

    var responseCode: Int = 0
    val api: String = "http://ec2-52-90-221-143.compute-1.amazonaws.com:8081/search/geocode?json={\"query\":\"" + URLEncoder.encode(address, "UTF-8") + "\"}"
    val url: URL = new URL(api)
    val httpConnection: HttpURLConnection = url.openConnection.asInstanceOf[HttpURLConnection]
    httpConnection.connect
    responseCode = httpConnection.getResponseCode
    if (responseCode == 200) {
      val in: BufferedReader = new BufferedReader(new InputStreamReader(httpConnection.getInputStream))
      var inputLine: String = null
      val tmp: StringBuffer = new StringBuffer

      while ({
        inputLine = in.readLine; inputLine
      } != null) {
        tmp.append(inputLine)
      }
      in.close

      val response = tmp.toString

      println(response)

      val jsonObject = response.parseJson.convertTo[TwoFishesJsonObject]

      if (jsonObject.interpretations.size>0) {
        val coordinates = jsonObject.interpretations.head.feature.geometry.center
        List(coordinates.lat.toString, coordinates.lng.toString)
      }
      else {
        List("NoCoords")
      }
    }
    else List(responseCode.toString)
  }


def printResponseFromTwoFishes(address: String) : Unit = {
  var responseCode: Int = 0
  //val api: String = "http://localhost:8081/search/geocode?json={\"query\":\"" + URLEncoder.encode(address, "UTF-8") + "\"}"
  val api: String = "http://ec2-52-90-221-143.compute-1.amazonaws.com:8081/search/geocode?json={\"query\":\"" + URLEncoder.encode(address, "UTF-8") + "\"}"
  val url: URL = new URL(api)
  val httpConnection: HttpURLConnection = url.openConnection.asInstanceOf[HttpURLConnection]
  httpConnection.connect
  responseCode = httpConnection.getResponseCode
  if (responseCode == 200) {
    //println(scala.io.Source.fromInputStream(httpConnection.getInputStream).mkString)
    val in: BufferedReader = new BufferedReader(new InputStreamReader(httpConnection.getInputStream))
    var inputLine: String = null
    val tmp: StringBuffer = new StringBuffer

    while ( {
      inputLine = in.readLine;
      inputLine
    } != null) {
      tmp.append(inputLine)
    }
    in.close

    val response = tmp.toString
    println(response)
  }
  else{
    println("No Response"+responseCode)
  }
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
    val stream = ssc.socketTextStream("localhost", 9999).filter(_.nonEmpty).map(x => {
      try {
        DataObjectFactory.createObject(x).asInstanceOf[twitter4j.Status]
      } catch {
        case ex: twitter4j.TwitterException =>{
          println("String not proper json exception\n"+x)
          null.asInstanceOf[twitter4j.Status]
        }

    }
    }).filter(_!=null)


    //stream.foreachRDD(l=>{l.foreach{t => println(t)}})

    /*
    val wholefile= sc.textFile("/Users/namitsharma/Downloads/dump2.txt").filter(_.nonEmpty)
      .map(x => DataObjectFactory.createObject(x).asInstanceOf[twitter4j.Status]).map(x=>x.getText)
    val yo = wholefile.collect;
    yo.map(t=>println(t));
    */

    /* Uncomment if you wanna start saving the tweets */
    //stream.saveAsTextFiles("./src/main/tweets/earthquake_tweets");



    //Filter based on words that cause False Positives
    val stream2 = stream.filter( x => {
      val tokenizedTweet = x.getText.split(" ").toList.map(_.toLowerCase)
      !((tokenizedTweet contains "concert") ||  (tokenizedTweet contains "conference") || (tokenizedTweet contains "lecture") || (tokenizedTweet contains "drill") || (tokenizedTweet contains "song") || (tokenizedTweet contains "predict") || (tokenizedTweet contains "video"))
    })


    //Declaring lazy NERClassifier
    var serializedClassifier: String = "src/main/resources/classifiers/english.all.3class.distsim.crf.ser.gz"
    object NER {
      @transient lazy val classifier: AbstractSequenceClassifier[CoreLabel] = CRFClassifier.getClassifier(serializedClassifier)
    }

    //Location Extraction using Stanford's NER, and filter the ones that don't have any location in them
    val stream3 = stream2
      .map(x => (x,extractLocations(NER.classifier.classifyWithInlineXML(x.getText))))//(twitter4j.Status,List[LocationString])
      .filter(x => {
        !(x._2.head equals "No Location")
      })

    //Todo
    //Remove commas     //s.map(c => if(c == '0') ' ' else c)
    //Capitalize all first letter of all words

    //Setup a locationsHaspMap RDD
    //val locationsHaspMap = sc.textFile("src/main/resources/coordinates");

    //Split record by location
    val stream4 = stream3.flatMap(t => splitForEachLocation(t._1,t._2))//(LocationString,twitter4j.Status)

    //Code for Clustering on a window goes here
    val myActivityStream = stream4
      //.map(x => (x._1,Set(x._2.getText))).reduceByKeyAndWindow((m: Set[String], n: Set[String]) => m | n,Seconds(12*60*60),Seconds(10))//(LocationString,Set(twitter4j.Status.getText))
      .map(x => (x._1,Set((getLatLongFromTwoFishes(x._1),x._2.getText)))).reduceByKeyAndWindow((m: Set[(List[String],String)], n: Set[(List[String],String)]) => m | n,Seconds(12*60*60),Seconds(10))//(LocationString,Set( (List(LatitudeString,LongitudeString),(twitter4j.Status.getText))))
      //.map(x => (x._1,Set(x._2.getText))).reduceByKeyAndWindow((m: Set[String], n: Set[String]) => m | n,Seconds(12*60*60),Seconds(10)).map(x=>(x._1,getLatLongFromTwoFishes(x._1),x._2))//(LocationString,List(LatitudeString,LongitudeString),Set(twitter4j.Status.getText))))
      //.map(x => (x._1,(getLatLongPositions(x._2.getText),x._2.getText))).window(Seconds(12*60*60),Seconds(10)).groupByKey()//(LocationString,Iterable<(List(LatitudeString,LongitudeString),(twitter4j.Status.getText))>)

    //Todo: Try to use inverse function and Enable checkpointing

    myActivityStream.foreachRDD(rdd => {
      println("\nMy Activity (%s)".format(rdd.count()))

      //val new_rdd = rdd.map(x => (x._2.size,x._1)).filter(_._1>=1).sortByKey(false) //(TweetCountByLocation,(Location,List(LatitudeString,LongitudeString))
      val new_rdd = rdd.map(x => (x._2.size,x)).sortByKey(false).filter(_._1>=1).map(x=>(x._1,x._2._1,x._2._2.head._1,x._2._2.head._2)) //(TweetCountByLocation,Location,List(LatitudeString,LongitudeString))
      //val new_rdd = rdd.map(x => (x._3.size,x)).sortByKey(false).filter(_._1>=1).map(x=>(x._1,x._2._1,x._2._2)) //(TweetCountByLocation,Location,List(LatitudeString,LongitudeString))
      //val new_rdd = rdd.map(x => (x._2.size,x)).sortByKey(false).filter(_._1>=1).map(x=>(x._1,x._2._1,x._2._2.head._1)) //(TweetCountByLocation,Location,List(LatitudeString,LongitudeString))

      new_rdd.foreach{t => println(t)}
      new_rdd.saveAsTextFile("./src/main/resources/new_saves_fromAWS");
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
