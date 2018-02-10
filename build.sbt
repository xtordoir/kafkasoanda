import Dependencies._

val kafka_streams_scala_version = "0.1.2"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.4",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "Hello",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "com.typesafe" % "config" % "1.3.1",
    libraryDependencies += "com.lightbend" %% "kafka-streams-scala" % "0.1.2",
    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.7",
    libraryDependencies += "com.twitter" %% "chill" % "0.9.2"
  )
