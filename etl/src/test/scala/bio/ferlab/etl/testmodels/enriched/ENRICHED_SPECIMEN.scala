package bio.ferlab.etl.testmodels.enriched

case class ENRICHED_SPECIMEN(participant_fhir_id: String = "P3",
                             sample_id: String = "BS_F6NDMZCN",
                             sample_fhir_id: String = "S3",
                             consent_type: Option[String] = None,
                             participant_id: String = "PT_48DYT4PP",
                             gender: String = "male",
                             is_proband: Boolean = true,
                             family_fhir_id: String = "G1",
                             family_id: String = "FM_NV901ZZN",
                             family: Option[ENRICHED_SPECIMEN_FAMILY] = Some(ENRICHED_SPECIMEN_FAMILY()),
                             affected_status: Boolean = false,
                             study_id: String = "SD_Z6MWD3H0",
                             study_code: String = "KF-CHDALL"
                            )

case class ENRICHED_SPECIMEN_FAMILY(`father_id`: String = "P2",
                                    `mother_id`: String = "P1")
