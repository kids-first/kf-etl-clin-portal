package model


case class SIMPLE_PARTICIPANT(
                               fhir_id: String = "38734",
                               participant_facet_ids: PARTICIPANT_FACET_IDS = PARTICIPANT_FACET_IDS(),
                               sex: String = "male",
                               ethnicity: String = "Not Reported",
                               race: String = "Not Reported",
                               external_id: String = "PAVKKD",
                               participant_id: String = "PT_48DYT4PP",
                               study_id: String = "SD_Z6MWD3H0",
                               release_id: String = "re_000001",
                               phenotype: Seq[PHENOTYPE] = Seq.empty,
                               observed_phenotype: Seq[PHENOTYPE_ENRICHED] = Seq.empty,
                               non_observed_phenotype: Seq[PHENOTYPE_ENRICHED] = Seq.empty,
                               diagnosis: Set[DIAGNOSIS] = Set.empty,
                               mondo: Seq[PHENOTYPE_ENRICHED] = Seq.empty,
                               outcomes: Seq[OUTCOME] = Seq.empty,
                               study: LIGHT_STUDY_CENTRIC = LIGHT_STUDY_CENTRIC(),
                               family: FAMILY = null,
                               family_type: String = "probant_only",
                               down_syndrome_status: String = "D21",
                               down_syndrome_diagnosis: Seq[String] = null,
                               is_proband: Boolean = false, // TODO
                               age_at_data_collection: Int = 111 // TODO
                             )
