val projectName = "dataservice-export"
val organization = "bio.ferlab"
val version = "1.0"
libraryDependencies ++= Seq(
  "org.asynchttpclient" % "async-http-client" % "2.12.2",
//  "org.json4s" %% "json4s-jackson" % "4.0.6",
//  "org.json4s" %% "json4s-ast" % "4.0.6",
  "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.0.3"
)


assembly / mainClass := Some("bio.ferlab.dataservice.etl.ExportTask")
assembly / assemblyJarName := "dataservice-export.jar"