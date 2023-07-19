package model

case class FAMILY_ENRICHED(
                            family_fhir_id: String = "f",
                            participant_fhir_id: String = "pfi",
                            proband_participant_id: String = "p",
                            relations: Seq[RELATION] = Seq.empty,
                          )

case class RELATION(`fhir_id`: String = "p", `role`: String = "father")

case class FAMILY_ROLES_TO_PROBAND(
                                    family_id: String = "f",
                                    relations_to_proband: Seq[RELATION] = Seq.empty,
                                  )