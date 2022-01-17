package model

case class DISEASE(
                        `fhir_id`: String = "678509",
                        `diagnosis_id`: String = "DG_KG6TQWCT",
                        `source_text`: String = "Acute lymphoblastic leukemia",
                        `source_text_tumor_location`: Seq[String] = Seq.empty[String],
                        `icd_id_diagnosis`: String = null,
                        `mondo_id_diagnosis`: String = null,
                        `ncit_id_diagnosis`: String = null,
                        `mondo`: OBSERVABLE_TERM = null,
//todo add age at event days
                      )


case class OBSERVABLE_TERM (
                             `name`: String = "HP:12345",
                             `parents`: Seq[String] = Seq.empty[String],
                             `is_tagged`: Boolean = false,
                             `is_leaf`: Boolean = false
                           )