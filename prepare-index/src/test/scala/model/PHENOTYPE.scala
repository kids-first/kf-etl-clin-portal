package model

case class PHENOTYPE(
                        `fhir_id`: String = "678509",
                        `hpo_phenotype_observed`: String = "Patent ductus arteriosus (HP_0001643)",
                        `hpo_phenotype_not_observed`: String = null,
                        `hpo_phenotype_observed_text`: String = "Patent ductus arteriosus",
                        `hpo_phenotype_not_observed_text`: String = null,
                        `observed`: Boolean = false
                    //todo add age at event days
                      )