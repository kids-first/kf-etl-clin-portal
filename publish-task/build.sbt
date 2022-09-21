val projectName = "publish-task"
val organization = "bio.ferlab"
val version = "1.0"

libraryDependencies ++= Seq(
  "bio.ferlab" %% "datalake-spark3" % "3.1.1",
  "org.json4s" %% "json4s-jackson" % "3.7.0-M5",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.2",
  "org.apache.httpcomponents" % "httpclient" % "4.5.13",
  "org.slf4j" % "slf4j-simple" % "1.7.36"
)

assembly / mainClass := Some("bio.ferlab.fhir.etl.PublishTask")
assembly / assemblyJarName := "publish-task.jar"