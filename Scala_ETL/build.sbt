name := "ETL"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.4"
libraryDependencies +=  "org.apache.hadoop" % "hadoop-client" % "3.3.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.3.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs-client" % "3.3.4"

libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.9.1"
  
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.10.1"

libraryDependencies += "org.postgresql" % "postgresql" % "42.4.3"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.295"

val sparkVersion = "3.2.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
libraryDependencies ++= Seq(
  "com.lihaoyi" %% "ujson" % "3.1.2"
)
libraryDependencies ++= Seq(
  "com.lihaoyi" %% "requests" % "0.8.0"
)
