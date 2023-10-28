name := "ETL"

version := "1.0"

scalaVersion := "2.12.15"

//fork := true
//libraryDependencies += "org.apach.hadoop" %% "hadoop-client" % "3.3.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.4"
//libraryDependencies +=  "org.apache.hadoop" % "hadoop-client" % "3.3.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.3.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs-client" % "3.3.4"

libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.9.1"
  
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.10.1"

val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
libraryDependencies ++= Seq(
  "com.lihaoyi" %% "ujson" % "3.1.2"
)
libraryDependencies ++= Seq(
  "com.lihaoyi" %% "requests" % "0.8.0"
)
