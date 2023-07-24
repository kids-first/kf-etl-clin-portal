/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2023-03-21T20:12:55.166785
 */
package bio.ferlab.etl.enrich.model


case class SPECIMEN_ENRICHED(participant_fhir_id: String = "P3",
                             sample_id: String = "BS_F6NDMZCN",
                             sample_fhir_id: String = "S3",
                             consent_type: Option[String] = None,
                             participant_id: String = "PT_48DYT4PP",
                             gender: String = "male",
                             is_proband: Boolean = true,
                             family_fhir_id: String = "G1",
                             family_id: String = "FM_NV901ZZN",
                             family: Option[SPECIMEN_FAMILY_ENRICHED] = Some(SPECIMEN_FAMILY_ENRICHED()),
                             affected_status: Boolean = false,
                             study_id: String = "SD_Z6MWD3H0",
                             study_code: String = "KF-COG-ALL"
                            )

case class SPECIMEN_FAMILY_ENRICHED(`father_id`: String = "P2",
                                    `mother_id`: String = "P1")
