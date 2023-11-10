val projectName = "etl"
val organization = "bio.ferlab"
val version = "1.0"

assembly / assemblyJarName := "etl.jar"

libraryDependencies += "com.lihaoyi" %% "ujson" % "3.0.0"
