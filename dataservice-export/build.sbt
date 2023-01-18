val projectName = "dataservice-export"
val organization = "bio.ferlab"
val version = "1.0"
libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.0.3"
)


assembly / mainClass := Some("bio.ferlab.dataservice.etl.ExportTask")
assembly / assemblyJarName := "dataservice-export.jar"