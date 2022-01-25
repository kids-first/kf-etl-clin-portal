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
                                `families`: Seq[PATIENT_FAMILY] = Seq.empty,
                                `release_id`: String = "re_000001"
                              )

case class PATIENT_FAMILY(
                           `fhir_id`: String = "42367",
                           `study_id`: String = "SD_Z6MWD3H0",
                           `family_id`: String = "FM_NV901ZZN",
                           `family_members`: Seq[(String, Boolean)] = Seq(("38734", false)),
                         )