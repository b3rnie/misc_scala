name := "misc"

version := "1.0"

scalaVersion := "2.13.1"
scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % "test"

libraryDependencies += "org.postgresql" % "postgresql" % "42.1.1"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

libraryDependencies += "com.typesafe" % "config" % "1.4.0"
libraryDependencies +=
"com.typesafe.akka" %% "akka-actor" % "2.6.1"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
