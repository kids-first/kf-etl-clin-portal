package bio.ferlab.fhir.etl

import org.apache.spark.sql.{SaveMode, SparkSession}

object Temp extends App {

  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._
  val path = "/home/adrian/projects/kf-etl-clin-portal-root/import-task/src/test"
  Seq(
    ("id", "partId", "toto", "obsId", 123, "SD_65064P2Z", "RE_000001")
  ).toDF("fhir_id", "participant_fhir_id", "vital_status", "observation_id", "age_at_event_days", "study_id", "release_id")
    .limit(0)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(path)

  spark.read.parquet(path).show(false)

}
