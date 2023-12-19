package bio.ferlab.etl.enriched.clinical

import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.etl.testmodels.normalized.{AGE_AT_EVENT, NORMALIZED_DISEASE, NORMALIZED_HISTOLOGY_OBS}
import bio.ferlab.etl.testutils.WithTestSimpleConfiguration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HistologyEnricherSpec extends AnyFlatSpec with Matchers with WithSparkSession with WithTestSimpleConfiguration {

  import spark.implicits._

  "transform" should "enrich histology with diseases" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_disease" -> Seq(
        NORMALIZED_DISEASE(`fhir_id` = "91984", `diagnosis_id` = "DG_45T24BMM", `participant_fhir_id` = "84608", `source_text` = "Oromandibular-Limb Hypogenesis Spectrum", `source_text_tumor_location` = Seq("Not Reported"), `mondo_code` = Some("MONDO:0017139"), `age_at_event` = AGE_AT_EVENT(`value` = 18, `unit` = "day")),
        NORMALIZED_DISEASE(`fhir_id` = "91989", `diagnosis_id` = "DG_J7JR6B13", `participant_fhir_id` = "84608", `source_text` = "Dermoid Cyst", `source_text_tumor_location` = Seq("Not Reported"), `mondo_code` = Some("MONDO:0002378"), `age_at_event` = AGE_AT_EVENT(`value` = 18, `unit` = "day")),
        NORMALIZED_DISEASE(`fhir_id` = "91986", `diagnosis_id` = "DG_W6XH06GP", `participant_fhir_id` = "84608", `source_text` = "Pda (Patent Ductus Arteriosus)", `source_text_tumor_location` = Seq("Not Reported"), `mondo_code` = Some("MONDO:0011827"), `age_at_event` = AGE_AT_EVENT(`value` = 18, `unit` = "day")),
        NORMALIZED_DISEASE(`fhir_id` = "91992", `diagnosis_id` = "DG_6D6NWQ0E", `participant_fhir_id` = "84608", `source_text` = "Hypoglossia-Hypodactyly Syndrome", `source_text_tumor_location` = Seq("Not Reported"), `mondo_code` = Some("MONDO:0007073"), `age_at_event` = AGE_AT_EVENT(`value` = 18, `unit` = "day")),
        NORMALIZED_DISEASE(`fhir_id` = "92012", `diagnosis_id` = "DG_3G0Z8JR9", `participant_fhir_id` = "84608", `source_text` = "Hypoglossia", `source_text_tumor_location` = Seq("Not Reported"), `mondo_code` = Some("MONDO:0015497"), `age_at_event` = AGE_AT_EVENT(`value` = 18, `unit` = "day")),
        NORMALIZED_DISEASE(`fhir_id` = "91985", `diagnosis_id` = "DG_G259B6MB", `participant_fhir_id` = "84608", `source_text` = "Multiple Congenital Anomalies", `source_text_tumor_location` = Seq("Not Reported"), `mondo_code` = Some("MONDO:0019042"), `age_at_event` = AGE_AT_EVENT(`value` = 18, `unit` = "day")),
      ).toDF(),
      "normalized_histology_observation" -> Seq(
        NORMALIZED_HISTOLOGY_OBS(`condition_id` = "91984", `specimen_id` = "100217", `patient_id` = "84608", `source_text_tumor_descriptor` = null),
        NORMALIZED_HISTOLOGY_OBS(`condition_id` = "91989", `specimen_id` = "100217", `patient_id` = "84608", `source_text_tumor_descriptor` = null),
        NORMALIZED_HISTOLOGY_OBS(`condition_id` = "91986", `specimen_id` = "100217", `patient_id` = "84608", `source_text_tumor_descriptor` = null),
        NORMALIZED_HISTOLOGY_OBS(`condition_id` = "91992", `specimen_id` = "100217", `patient_id` = "84608", `source_text_tumor_descriptor` = null),
        NORMALIZED_HISTOLOGY_OBS(`condition_id` = "92012", `specimen_id` = "100217", `patient_id` = "84608", `source_text_tumor_descriptor` = null),
        NORMALIZED_HISTOLOGY_OBS(`condition_id` = "91985", `specimen_id` = "100217", `patient_id` = "84608", `source_text_tumor_descriptor` = null),
      ).toDF(),

    )

    val output = HistologyEnricher(defaultRuntime, List("SD_Z6MWD3H0")).transform(data)

    val ehdDF = output("enriched_histology_disease")

    val EXPECTED_N_OF_ROWS = 6
    ehdDF.count shouldEqual EXPECTED_N_OF_ROWS
    ehdDF.select(col("condition_id")).distinct.count shouldEqual EXPECTED_N_OF_ROWS
  }
}

