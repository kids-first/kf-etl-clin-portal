val projectName = "import-task"
val organization = "bio.ferlab"
val version = "1.0"

scalaVersion in ThisBuild := "2.12.14"

libraryDependencies ++= Seq(
  "bio.ferlab" %% "datalake-spark31" % "0.2.13",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.spark" %% "spark-hive" % "3.1.2",
  "org.apache.spark" %% "spark-avro" % "3.1.2",
  "org.apache.hadoop" % "hadoop-client" % "3.2.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.2.0",
  "io.delta" %% "delta-core" % "1.0.0",
  "com.typesafe.play" %% "play-json" % "2.9.2",

  // Scala module 2.10.0 requires Jackson Databind version >= 2.10.0 and < 2.11.0
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.2",

  // TEST
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

assembly / assemblyMergeStrategy:= {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assembly / test := {}
assembly / mainClass := Some("bio.ferlab.fhir.etl.ImportTask")
assembly / assemblyJarName := "import-task.jar"