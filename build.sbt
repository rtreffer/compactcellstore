organization := "de.measite"

name         := "compactstore"

version      := "1.0-SNAPSHOT"

scalaVersion := "2.10.3"

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)

resolvers += "Base" at "http://repo1.maven.org/maven2/"

resolvers += "SonatypeReleases" at "http://oss.sonatype.org/content/repositories/releases/"

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.11" % "test",
  "org.scalatest" %% "scalatest" % "2.0" % "test",
  "org.clapper" %% "argot" % "1.0.1"
)

mainClass := Some("de.measite.compactstore.CellIdRewrite")
