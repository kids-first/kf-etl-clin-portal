val projectName = "enrich"
val organization = "bio.ferlab"
val version = "1.0"

assembly / mainClass := Some("bio.ferlab.enrich.etl.Enrich")
assembly / assemblyJarName := "enrich.jar"