ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2"
)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test

libraryDependencies += "org.typelevel" %% "cats-core" % "2.9.0"
javaHome := Some(file("C:\\jdk-11.0.0.1"))
javacOptions ++= Seq("-source", "11", "-target", "11")
lazy val root = (project in file("."))
  .settings(
    name := "ApacheSpark"

    )

