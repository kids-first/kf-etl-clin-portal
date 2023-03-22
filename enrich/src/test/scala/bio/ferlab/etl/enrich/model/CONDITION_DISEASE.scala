package bio.ferlab.etl.enrich.model

case class CONDITION_DISEASE(
                              `fhir_id`: String = "676049",
                              `diagnosis_id`: String = "DG_KG6TQWCT",
                              `condition_coding`: Seq[CONDITION_CODING] = Seq.empty[CONDITION_CODING],
                              `source_text`: String = "Acute lymphoblastic leukemia",
                              `participant_fhir_id`: String = "38722",
                              `source_text_tumor_location`: Seq[String] = Seq.empty[String],
                              `uberon_id_tumor_location`: Seq[String] = Seq.empty,
                              `affected_status`: Boolean = false,
                              `affected_status_text`: String = null,
                              `mondo_id`: Option[String] = None,
                              `age_at_event`: AGE_AT_EVENT = AGE_AT_EVENT(),
                              `study_id`: String = "SD_Z6MWD3H0",
                              `release_id`: String = "re_000001"
                            )

case class CONDITION_CODING(
                             `category`: String = "ICD",
                             `code`: String = "C91.0"
                           )

case class AGE_AT_EVENT(
                         `value`: Int = 0,
                         `unit`: String = "day",
                         `from_event`: String = "Birth"
                       )