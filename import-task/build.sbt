

val projectName = "import-task"
val organization = "bio.ferlab"
val version = "1.0"
scalaVersion in ThisBuild := "2.12.14"

val awsVersion = "2.17.81"
val spark31Version = "3.1.2"
val slf4jVersion = "1.7.32"

libraryDependencies ++= Seq(
  "bio.ferlab" % "fhavro" % "0.0.10-SNAPSHOT",
  "bio.ferlab" %% "datalake-spark31" % "0.1.7",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.spark" %% "spark-hive" % "3.1.2",
  "org.apache.spark" %% "spark-avro" % "3.1.2",
  "org.apache.hadoop" % "hadoop-client" % "3.2.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.2.0",
  "io.delta" %% "delta-core" % "1.0.0",
  "org.typelevel" %% "cats-core" % "2.3.1",
  "com.typesafe.play" %% "play-json" % "2.9.2",
  "com.github.pureconfig" %% "pureconfig" % "0.15.0",

  // Scala module 2.10.0 requires Jackson Databind version >= 2.10.0 and < 2.11.0
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.2",

  // TEST
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  "Sonatype OSS Releases" at "https://s01.oss.sonatype.org/content/repositories/snapshots/",
  "aws-glue-etl-artifacts" at "https://aws-glue-etl-artifacts.s3.amazonaws.com/release/"
)

fork in ThisBuild := true
parallelExecution in ThisBuild := false
logBuffered in ThisBuild := false
testOptions in ThisBuild += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

javaOptions ++= Seq(
  "-XX:+CMSClassUnloadingEnabled",
  "-XX:MaxMetaspaceSize=512M",
  "-XX:MetaspaceSize=256M",
  "-Xms512M",
  "-Xmx2G",
  "-XX:MaxPermSize=2048M"
)

assembly / assemblyMergeStrategy:= {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assembly / test := {}
assembly / mainClass := Some("bio.ferlab.fhir.etl.ImportTask")
assembly / assemblyJarName := "import-task.jar"