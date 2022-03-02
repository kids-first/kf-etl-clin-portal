val projectName = "import-task"
val organization = "bio.ferlab"
val version = "1.0"

scalaVersion in ThisBuild := "2.12.14"

libraryDependencies ++= Seq(
  "bio.ferlab" %% "datalake-spark31" % "0.2.20",
  "org.slf4j" % "slf4j-simple" % "1.7.36"
)