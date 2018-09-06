package com.ctm

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.StatusCodes.OK
import akka.stream.scaladsl.{RestartSource, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.ctm.modules.{OAuthHeader, Tweet}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.TimeoutException
import scala.util.control.NonFatal
import scala.concurrent.duration._

object Main extends App with StrictLogging {

  private val requestParams = Map("track" -> "insurance")
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

  val conf = ConfigFactory.load()
  val oAuthHeader = OAuthHeader(conf, requestParams)
  val httpRequest = createHttpRequest(oAuthHeader, Uri(conf.getString("twitter.url")))


  val restartSource = RestartSource.withBackoff(
    minBackoff = 3.seconds,
    maxBackoff = 30.seconds,
    randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
  ) { () =>
    Source.fromFutureSource {
      val response = Http().singleRequest(httpRequest)

      response.failed.foreach(t => System.err.println(s"Request has been failed with $t"))

      response.map(resp => {
        resp.status match {
          case OK => resp.entity.withoutSizeLimit().dataBytes
          case code =>
            val text = resp.entity.dataBytes.map(_.utf8String)
            val error = s"Unexpected status code: $code, $text"
            System.err.println(error)
            Source.failed(new RuntimeException(error))
        }
      })
    }
  }

  restartSource
    .idleTimeout(idleDuration)
    .scan("")((acc, curr) =>
      if (acc.contains(docDelimiter)) curr.utf8String
      else acc + curr.utf8String
    )
    .filter(_.contains(docDelimiter)).async
    .log("json", s => s)
    .map(json => parse(json).extract[Tweet])
    .runForeach(println)


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
    println(http.toString())

    http
  }

}
