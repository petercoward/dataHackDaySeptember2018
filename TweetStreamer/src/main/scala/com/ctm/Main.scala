package com.ctm

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.StatusCodes.OK
import akka.stream.scaladsl.{RestartSource, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.ctm.modules.{CloudWatch, OAuthHeader, Tweet => usTweet}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.json4s._
import org.json4s.native.JsonMethods._
import com.danielasfregola.twitter4s.{TwitterRestClient, TwitterStreamingClient}
import com.danielasfregola.twitter4s.entities.{AccessToken, ConsumerToken, HashTag, Tweet}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.TimeoutException
import scala.util.control.NonFatal
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Main extends App with StrictLogging {

  private val requestParams = Map("track" -> "brexit")
  private val docDelimiter = "\r\n"
  val uniqueBuckets = 500
  val topCount = 15
  val idleDuration: FiniteDuration = 90 seconds

  // json
  implicit val formats: DefaultFormats.type = DefaultFormats

  // akka
  implicit val system: ActorSystem = ActorSystem("TweetsWordCount")

  val decider: Supervision.Decider = {
    case _: TimeoutException => Supervision.Restart
    case NonFatal(e) =>
      logger.error(s"Stream failed with ${e.getMessage}, going to resume")
      Supervision.Resume
  }

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  val cloudwatch = CloudWatch("hackday2018", "test")

  val conf = ConfigFactory.load()
  val oAuthHeader = OAuthHeader(conf, requestParams)
  val httpRequest = createHttpRequest(oAuthHeader, Uri(conf.getString("twitter.url")))


  val consumerToken = ConsumerToken(key = "fJnDYharsaWT8B2NKNTA8iFqD", secret = "Efc7dnP24SoFEEtIhbWhB4cb2OsjsmMrQtcuacVKHomV8NDo9S")
  val accessToken = AccessToken(key = "2356886993-OGyvYujDFUyg1tQTE5Y7hTLb3tqRNsVAijtHEvh", secret = "Y8TbZf88EST3fYAU8p2LZzGSoI0p06lRd6waEQv7efa4P")
  val restClient = TwitterRestClient(consumerToken, accessToken)
  val streamingClient = TwitterStreamingClient(consumerToken, accessToken)

  var hashtag_map: collection.mutable.Map[String, Int] = new mutable.HashMap()
  var tweet_count = 0


  val stream = streamingClient.filterStatuses(tracks = Seq("trump")) {
    case tweet: Tweet =>
      tweet_count += 1
      val extendedHashtags = tweet.extended_entities.map(_.hashtags).getOrElse(Seq.empty)
      val hashtags = tweet.entities.map(_.hashtags).getOrElse(Seq.empty)
      hashtags.foreach { hshTg =>
        if (hashtag_map.contains(hshTg.text.toLowerCase)) {
          val counter = hashtag_map(hshTg.text.toLowerCase) + 1
          hashtag_map = hashtag_map.updated(hshTg.text.toLowerCase, counter)
        } else {
          hashtag_map = hashtag_map.updated(hshTg.text.toLowerCase, 1)
        }
        println(s"Hashtag: ${hshTg.text.toLowerCase}")
      }
      extendedHashtags.foreach { hshTg =>
        if (hashtag_map.contains(hshTg.text.toLowerCase)) {
          val counter = hashtag_map(hshTg.text.toLowerCase) + 1
          hashtag_map = hashtag_map.updated(hshTg.text.toLowerCase, counter)
        } else {
          hashtag_map = hashtag_map.updated(hshTg.text.toLowerCase, 1)
        }
        println(s"Hashtag: ${hshTg.text.toLowerCase}")
      }

//      if(tweet_count == 1000){
//        println("Tweet count: " + tweet_count)
//        println("Sorted contents of the map: ")
//        hashtag_map.toSeq.sortWith(_._2 > _._2).foreach(println)
//        Thread.sleep(10000)
//        tweet_count = 0
//      }

      tweet



  }

  Thread.sleep(100000)


  streamingClient.shutdown()

  println("Tweet count: " + tweet_count)
  println("Sorted contents of the map: ")
  hashtag_map.toSeq.sortWith(_._2 > _._2).foreach(println)
  //mutable.ListMap(hashtag_map.toSeq.sortWith(_._2 > _._2):_*).foreach(println)

//  val res = restClient.searchTweet("meerkatmovies", count = 20000)
//  res.onComplete {
//    case Success(x) => x.data.statuses.foreach(r => println(s"Printing retrieved data: ${r.text}"))
//    case Failure(e) => println(e)
//  }


//  val restartSource = RestartSource.withBackoff(
//    minBackoff = 3.seconds,
//    maxBackoff = 30.seconds,
//    randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
//  ) { () =>
//    Source.fromFutureSource {
//      val response = Http().singleRequest(httpRequest)
//
//      response.failed.foreach(t => System.err.println(s"Request has been failed with $t"))
//
//      response.map(resp => {
//        resp.status match {
//          case OK => resp.entity.withoutSizeLimit().dataBytes
//          case code =>
//            val text = resp.entity.dataBytes.map(_.utf8String)
//            val error = s"Unexpected status code: $code, $text"
//            System.err.println(error)
//            Source.failed(new RuntimeException(error))
//        }
//      })
//    }
//  }
//
//  restartSource
//    .idleTimeout(idleDuration)
//    .scan("")((acc, curr) =>
//      if (acc.contains(docDelimiter)) curr.utf8String
//      else acc + curr.utf8String
//    )
//    .filter(_.contains(docDelimiter)).async
//    .log("json", s => s)
//    .map(json => parse(json).extract[usTweet])
//    .runForeach(println)


  private def createHttpRequest(header: String, source: Uri): HttpRequest = {
    val httpHeaders = List(
      HttpHeader.parse("Authorization", header) match {
        case ParsingResult.Ok(h, _) => Some(h)
        case _ => None
      },
      HttpHeader.parse("Accept", "*/*") match {
        case ParsingResult.Ok(h, _) => Some(h)
        case _ => None
      }
    ).flatten

    val http = HttpRequest(
      method = HttpMethods.POST,
      uri = source,
      headers = httpHeaders,
      entity = HttpEntity(
        contentType = ContentType(MediaTypes.`application/x-www-form-urlencoded`, HttpCharsets.`UTF-8`),
        string = requestParams.map { case (k, v) => s"$k=$v" }.mkString("&")
      ).withoutSizeLimit()
    )
    http
  }

}
