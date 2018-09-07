package com.ctm.modules

import java.io

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.StrictLogging

class LocationFlow extends StrictLogging {
  def flow: Flow[Tweet, Tweet, NotUsed] =
    Flow[Tweet]
    .map {
      case twt =>
        val coordinates = twt.coordinates.getOrElse()
        coordinates match {
          case Some(coord: Seq[Float]) =>
            val lat = coord.head
            val long = coord.toList.last
          case None => None
        }
        //logger.info()
        twt
    }
}
