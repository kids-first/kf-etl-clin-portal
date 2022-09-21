val projectName = "import-task"
val organization = "bio.ferlab"
val version = "1.0"

ThisBuild / scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "bio.ferlab" %% "datalake-spark3" % "3.1.1",
  "org.slf4j" % "slf4j-simple" % "1.7.36"
)
