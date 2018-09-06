package com.ctm.modules

import akka.actor.ActorSystem
import cloud.drdrdr.oauth._
import com.typesafe.config.Config

object OAuthHeader {
  def apply(conf: Config, params: Map[String, String])(implicit system: ActorSystem): String = {
    val oauth = new Oauth(key = conf.getString("twitter.auth.consumerKey"), secret = conf.getString("twitter.auth.consumerSecret"))
    oauth.setAccessTokens(conf.getString("twitter.auth.accessToken"), conf.getString("twitter.auth.accessTokenSecret"))
    oauth.getSignedHeader(conf.getString("twitter.url"), "POST", params)
  }
}
