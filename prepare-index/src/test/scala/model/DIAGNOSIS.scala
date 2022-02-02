package model

case class DIAGNOSIS(
                      `fhir_id`: String = "438351",
                      `diagnosis_id`: String = "DG_KG6TQWCT",
                      `source_text`: String = "Acute lymphoblastic leukemia",
                      `source_text_tumor_location`: Seq[String] = Seq.empty,
                      `uberon_id_tumor_location`: Seq[Seq[CODING]] = Seq.empty,
                      `affected_status`: Boolean = false,
                      `affected_status_text`: String = null,
                      `age_at_event_days`: Int = 0,
                      `icd_id_diagnosis`: String = null,
                      `mondo_id_diagnosis`: String = null,
                      `ncit_id_diagnosis`: String = null
                    )

case class CODING(
                   `id`: String = null,
                   `system`: String = null,
                   `version`: String = null,
                   `code`: String = null,
                   `display`: String = null,
                   `userSelected`: Boolean = false
                 )