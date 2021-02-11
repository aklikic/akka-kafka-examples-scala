import sbt._

object Dependencies {
  val scalaVer = "2.13.3"
  // #deps
  val AkkaVersion = "2.6.11"
  //val AkkaHttpVersion = "10.1.12"
  val AlpakkaKafkaVersion = "2.0.5"
  // #deps
  val dependencies = List(
    // #deps
    "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
    "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
    //"com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-stream-kafka" % AlpakkaKafkaVersion,
    "com.typesafe.akka" %% "akka-discovery" % AkkaVersion,

    // Logging
    "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    // #deps

    "org.testcontainers" % "kafka" % "1.15.1",

    "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
    "com.google.guava" % "guava" % "28.2-jre" % Test,
    "junit" % "junit" % "4.13" % Test

  )
}
