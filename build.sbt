ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.apache.spark" %% "spark-streaming" % "3.4.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.3.2",
  "org.apache.kafka" % "kafka-clients" % "3.4.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2",
  "org.apache.hadoop" % "hadoop-client" % "3.3.2",
  "org.apache.hadoop" % "hadoop-hdfs-test" % "0.22.0",
  "org.apache.spark" %% "spark-hive" % "3.3.2" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.apache.avro" % "avro" % "1.11.0"
)
resolvers ++= Seq(
  "confluent" at "https://packages.confluent.io",
  Resolver.mavenLocal
)

libraryDependencies ++= Seq(
  "io.confluent" % "kafka-avro-serializer" % "7.4.0",
  "org.apache.spark" %% "spark-avro" % "3.4.0"
)
resolvers += "Confluent Maven Repo" at "https://packages.confluent.io/maven/"
resolvers += "Confluent Avro Serializer" at "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/"

javaHome := Some(file("C:\\Program Files\\Java\\jdk-19"))
javacOptions ++= Seq("-source", "19", "-target", "19")

lazy val root = (project in file("."))
  .settings(
    name := "ApacheSpark"
  )
