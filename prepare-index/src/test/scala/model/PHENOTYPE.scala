package model

case class PHENOTYPE(
                      `fhir_id`: String = "678509",
                      `hpo_phenotype_observed`: String = "Acute lymphoblastic leukemia (HP_0001631)",
                      `hpo_phenotype_not_observed`: String = null,
                      `hpo_phenotype_observed_text`: String = "Acute lymphoblastic leukemia",
                      `hpo_phenotype_not_observed_text`: String = null,
                      `observed`: Boolean = false,
                      `age_at_event_days`: Int = 0
                    )

case class PHENOTYPE_MONDO(
                            `name`: String = "Abnormality of the cardiovascular system (HP:0001626)",
                            `parents`: Seq[String] = Seq.empty,
                            `is_tagged`: Boolean = false,
                            `is_leaf`: Boolean = false,
                            `age_at_event_days`: Seq[Int] = Seq.empty
                          )