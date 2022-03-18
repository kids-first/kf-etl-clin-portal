package model

case class GROUP(
                   fhir_id: String = "42367",
                   study_id: String = "SD_Z6MWD3H0",
                   family_id: String = "FM_NV901ZZN",
                   `type`: String = "person",
                   family_members: Seq[(String, Boolean)] = Seq(("38734", false)),
                   family_members_id: Seq[String] = Seq("38734")
                 )
case class FAMILY(
                   fhir_id: String = "42367",
                   family_id: String = "FM_NV901ZZN",
                   father_id: Option[String] = None,
                   mother_id: Option[String] = None,
                   family_relations: Seq[FAMILY_RELATIONS] = Seq(FAMILY_RELATIONS())
                 )

case class FAMILY_RELATIONS(
                   related_participant_id: String = "PT_48DYT4PP",
                   related_participant_fhir_id: String = "123",
                   relation: String = "mother"
                 )
