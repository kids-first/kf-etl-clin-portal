package model

case class CONDITION_DISEASE (
                      `fhir_id`: String = "676049",
                      `study_id`: String = "SD_Z6MWD3H0",
                      `diagnosis_id`: String = "DG_KG6TQWCT",
                      `condition_coding`: Seq[CONDITION_CODING] = Seq.empty[CONDITION_CODING],
                      `source_text`: String = "Acute lymphoblastic leukemia",
                      `participant_fhir_id`: String = "38722",
                      `source_text_tumor_location`: Seq[String] = Seq.empty[String],
//                      `uberon_id_tumor_location`: String = null, //TODO check in importtask why this is a Seq[Seq[elements]]
                      `age_at_event`: AGE_AT_EVENT = AGE_AT_EVENT(),
                     )


case class CONDITION_CODING (
                              `category`: String = "ICD",
                              `code`: String =  "C91.0"
                            )

case class AGE_AT_EVENT (
                          `value`: Int = 0,
                          `unit`: String =  "day",
                          `from_event`: String =  "Birth"
                        )