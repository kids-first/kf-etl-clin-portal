package model

case class PATIENT_WITH_FAMILY(
                                `fhir_id`: String = "38734",
                                `gender`: String = "male",
                                `ethnicity`: String = "Not Reported",
                                `race`: String = "Not Reported",
                                `external_id`: String = "PAVKKD",
                                `participant_id`: String = "PT_48DYT4PP",
                                `study_id`: String = "SD_Z6MWD3H0",
                                `families_id`: Seq[String] = Seq.empty,
                                `families`: Seq[FAMILY] = Seq.empty
                              )