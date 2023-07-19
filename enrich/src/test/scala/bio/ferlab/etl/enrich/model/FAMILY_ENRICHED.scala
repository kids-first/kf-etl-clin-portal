package bio.ferlab.etl.enrich.model

case class FAMILY_ENRICHED(
                            family_fhir_id: String = "f",
                            proband_participant_id: String = "p",
                            relations: Seq[RELATION] = Seq.empty,
                          )

case class RELATION(`fhir_id`: String = "p", `role`: String = "father")