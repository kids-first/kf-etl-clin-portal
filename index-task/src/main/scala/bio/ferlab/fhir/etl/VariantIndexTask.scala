package bio.ferlab.fhir.etl

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, SimpleConfiguration}
import bio.ferlab.datalake.spark3.elasticsearch.{ElasticSearchClient, Indexer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.generic.auto._

object VariantIndexTask extends App {

  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(
  esNodes, // http://localhost:9200
  esPort, // 9200
  release_id, // release id
  jobType, // variant_centric, variants_suggestions, gene_centric, genes_suggestions
  configFile, // config/qa-[project].conf or config/prod.conf or config/dev-[project].conf
  input,
  chromosome
  ) = args

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources[SimpleConfiguration](configFile)

  private val esUsername = sys.env.get("ES_USERNAME")
  private val esPassword = sys.env.get("ES_PASSWORD")

  private val defaultEsConfigs = Map(
    "es.index.auto.create" -> "true",
    "es.net.ssl" -> "true",
    "es.net.ssl.cert.allow.self.signed" -> "true",
    "es.nodes" -> esNodes,
    "es.nodes.wan.only" -> "true",
    "es.wan.only" -> "true",
    "spark.es.nodes.wan.only" -> "true",
    "es.port" -> esPort)

  private val esConfigs = esUsername.map(username =>
    defaultEsConfigs ++ Map("es.net.http.auth.user" -> username, "es.net.http.auth.pass" -> esPassword.get)
  ).getOrElse(defaultEsConfigs)

  private val sparkConfigs: SparkConf =
    (conf.sparkconf ++ esConfigs)
      .foldLeft(new SparkConf()) { case (c, (k, v)) => c.set(k, v) }

  implicit val spark: SparkSession = SparkSession.builder
    .config(sparkConfigs)
    .enableHiveSupport()
    .appName(s"IndexTask")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val templatePath = s"${conf.storages.find(_.id == "storage").get.path}/templates/template_$jobType.json"

  implicit val esClient: ElasticSearchClient = new ElasticSearchClient(esNodes.split(',').head, esUsername, esPassword)

  val indexName = chromosome match {
    case "all" => s"${jobType}_$release_id".toLowerCase
    case c => s"${jobType}_${c}_${release_id}".toLowerCase
  }



  println(s"Run Index Task to fill index $indexName")

  val df: DataFrame = chromosome match {
    case "all" =>
      spark.read
        .parquet(input)
    case chr =>
      spark.read
        .parquet(input)
        .where(col("chromosome") === chr)
  }


  new Indexer("index", templatePath, indexName)
    .run(df)


}
