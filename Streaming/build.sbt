ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.0"

val dependencies = Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
  "com.lihaoyi" %% "os-lib" % "0.9.1",

  "mysql" % "mysql-connector-java" % "5.1.2",
  "org.postgresql" % "postgresql" % "42.4.3"
)

libraryDependencies ++= dependencies
