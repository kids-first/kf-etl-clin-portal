package bio.ferlab.fhir.etl.fhavro

import org.apache.spark.sql.DataFrame

object FhavroCustomOperations {
  implicit class DataFrameOps(dataFrame: DataFrame) {
    def withPatientExtension: DataFrame = {
      // TODO Extract race & ethnicity from the Patient.extension[].extension[]
      null
    }
  }
}
