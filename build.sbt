import sbtassembly.AssemblyPlugin.autoImport.assembly
val datalakeLibVersion = "14.0.0"

lazy val fhavro_export = project in file("fhavro-export")

val sparkDepsSetting = Seq(
  libraryDependencies ++= Seq(
    "bio.ferlab" %% "datalake-spark3" % datalakeLibVersion,
    "bio.ferlab" %% "datalake-test-utils" % datalakeLibVersion % Test,
    "org.apache.spark" %% "spark-sql" % "3.3.2" % Provided, //emr-6.11.0
    "org.apache.spark" %% "spark-hive" % "3.3.2" % Provided, //emr-6.11.0
    "org.apache.hadoop" % "hadoop-client" % "3.3.3" % Provided, //emr-6.11.0
    "org.apache.hadoop" % "hadoop-aws" % "3.3.3" % Provided, //emr-6.11.0
    "io.delta" %% "delta-core" % "2.2.0" % Provided, //emr-6.11.0
    "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.0.3" % Provided, //Used by dataservice normalize
    "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  )
)

val commonSettings = Seq(
  scalaVersion := "2.12.14",
  Compile / unmanagedResourceDirectories += config.base / "output",
  assembly / assemblyShadeRules := Seq(
    ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inAll
  ),
  assembly / assemblyMergeStrategy := {
    case "META-INF/io.netty.versions.properties" => MergeStrategy.last
    case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
    case "META-INF/DISCLAIMER" => MergeStrategy.last
    case "mozilla/public-suffix-list.txt" => MergeStrategy.last
    case "overview.html" => MergeStrategy.last
    case "git.properties" => MergeStrategy.discard
    case "mime.types" => MergeStrategy.first
    case "module-info.class" => MergeStrategy.first
    case PathList("scala", "annotation", "nowarn.class" | "nowarn$.class") => MergeStrategy.first
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  },
  assembly / test := {},
  resolvers += "Sonatype OSS Releases" at "https://s01.oss.sonatype.org/content/repositories/releases"


)
lazy val config = (project in file("config")).settings(sparkDepsSetting)

lazy val etl = (project in file("etl")).dependsOn(config).settings(commonSettings ++ sparkDepsSetting)