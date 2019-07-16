
name := "stream-hbase"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.3"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.3"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.3"

//libraryDependencies += "org.apache.hbase" % "hbase-common" % "2.0.0"

libraryDependencies += "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11"

libraryDependencies += "com.google.code.gson" % "gson" % "1.7.1"

scalacOptions += "-target:jvm-1.8"
