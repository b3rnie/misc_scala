name := "misc"

version := "1.0"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "postgresql" % "postgresql" % "9.2-1002.jdbc4"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

