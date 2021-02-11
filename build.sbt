import sbt.Keys._

name := "akka-kafka-examples-scala"
organization := "com.lightbend.akka"
version := "1.0.0"
scalaVersion := Dependencies.scalaVer
libraryDependencies ++= Dependencies.dependencies

fork in run := true
connectInput in run := true
