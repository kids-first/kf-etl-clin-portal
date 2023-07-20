package model

case class FAMILY_ENRICHED(
                            family_fhir_id: String = "f",
                            participant_fhir_id: String = "pfi",
                            relations: Seq[RELATION] = Seq.empty,
                          )

case class RELATION(`participant_id`: String = "p", `role`: String = "father")

case class FAMILY(
                   family_id: String = "f",
                   relations_to_proband: Seq[RELATION] = Seq.empty,
                 )