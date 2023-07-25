package bio.ferlab.etl.testutils

import bio.ferlab.datalake.commons.config.RunStep
import bio.ferlab.fhir.etl.config.{ETLConfiguration, KFRuntimeETLContext}
import org.apache.spark.sql.SparkSession

class KFTestETLContext(steps: Seq[RunStep] = Nil)(implicit configuration: ETLConfiguration, sparkSession: SparkSession) extends KFRuntimeETLContext("path", steps = "", appName = Some("Spark Test")) {
  override lazy val config: ETLConfiguration = configuration
  override lazy val spark: SparkSession = sparkSession
  override lazy val runSteps: Seq[RunStep] = steps
}

object KFTestETLContext {
  def apply(runSteps: Seq[RunStep] = Nil)(implicit configuration: ETLConfiguration, sparkSession: SparkSession): KFRuntimeETLContext = {
    new KFTestETLContext(runSteps)(configuration, sparkSession)
  }
}