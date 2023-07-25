package bio.ferlab.etl.testmodels.normalized

case class NORMALIZED_PHENOTYPE(
                                `fhir_id`: String = "676049",
                                `study_id`: String = "SD_Z6MWD3H0",
                                `phenotype_id`: String = "DG_KG6TQWCT",
                                `condition_coding`: Seq[CONDITION_CODING] = Seq.empty[CONDITION_CODING],
                                `source_text`: String = "Acute lymphoblastic leukemia",
                                `participant_fhir_id`: String = "38722",
                                `observed`: String = null,
                                `age_at_event`: AGE_AT_EVENT = AGE_AT_EVENT(),
                              )
