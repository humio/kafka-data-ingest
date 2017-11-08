name := "humio-ingest"

version := "0.1"

scalaVersion := "2.11.11"

javaHome := sys.env.get("JAVA_HOME") map file


//core dependencies
libraryDependencies ++= Seq(
  "org.json" % "json" % "20141113",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.6",
  "io.dropwizard.metrics" % "metrics-core" % "3.1.2",
  "commons-io" % "commons-io" % "2.5",
  "org.apache.commons" % "commons-lang3" % "3.4",
  "com.typesafe.akka" %% "akka-http-core" % "10.0.10",
  "com.typesafe.akka" %% "akka-http" % "10.0.10",
  "com.typesafe.akka" %% "akka-http-testkit" % "10.0.10",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.10",
  "org.apache.kafka" % "kafka_2.11" % "0.8.2.2"
    exclude("log4j", "log4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
)

mainClass in Compile := Some("com.humio.ingest.main.Runner")

javaOptions ++= Seq("-Xmx4G","-Xms1G")

fork := true
