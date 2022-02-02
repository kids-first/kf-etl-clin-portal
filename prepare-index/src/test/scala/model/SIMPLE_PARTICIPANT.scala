package model


case class SIMPLE_PARTICIPANT(
                               `fhir_id`: String = "38734",
                               `sex`: String = "male",
                               `ethnicity`: String = "Not Reported",
                               `race`: String = "Not Reported",
                               `external_id`: String = "PAVKKD",
                               `participant_id`: String = "PT_48DYT4PP",
                               `study_id`: String = "SD_Z6MWD3H0",
                               `release_id`: String = "re_000001",
                               `phenotype`: Seq[PHENOTYPE] = Seq.empty,
                               `observed_phenotype`: Seq[PHENOTYPE_MONDO] = Seq.empty,
                               `non_observed_phenotype`: Seq[PHENOTYPE_MONDO] = Seq.empty,
                               `diagnosis`: Seq[DIAGNOSIS] = Seq.empty,
                               `mondo`: Seq[PHENOTYPE_MONDO] = Seq.empty,
                               `outcomes`: Seq[OUTCOME] = Seq.empty,
                               `families_id`: Seq[String] = Seq.empty,
                               `families`: Seq[FAMILY] = Seq.empty,
                               `karyotype`: String = "TODO",
                               `down_syndrome_diagnosis`: String = "TODO",
                               `family_type`: String = "TODO",
                               `is_proband`: Boolean = false, // TODO
                               `age_at_data_collection`: Int = 111 // TODO
                             )
