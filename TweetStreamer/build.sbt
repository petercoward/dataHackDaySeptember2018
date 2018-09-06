name := "TweetStreamer"

version := "0.1"

scalaVersion := "2.12.6"

val scalaLoggingVersion = "3.7.2"
val logbackVersion = "1.2.3"
val loggingScala = "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion
val loggingLogback = "ch.qos.logback" % "logback-classic" % logbackVersion
val akkaStreamsV = "2.5.6"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaStreamsV,
  "com.typesafe.akka" %% "akka-http" % "10.0.10",
  "org.json4s" %% "json4s-native" % "3.5.0",
  // needs to be built and published locally
  "cloud.drdrdr" %% "oauth-headers" % "0.3",
  "org.typelevel" %% "cats-core" % "1.0.0-RC1",
  loggingScala,
  loggingLogback
)
