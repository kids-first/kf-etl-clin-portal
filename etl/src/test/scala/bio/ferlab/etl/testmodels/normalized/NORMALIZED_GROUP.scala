package bio.ferlab.etl.testmodels.normalized

case class NORMALIZED_GROUP(
                   fhir_id: String = "42367",
                   study_id: String = "SD_Z6MWD3H0",
                   family_id: String = "FM_NV901ZZN",
                   `type`: String = "person",
                   family_members: Seq[(String, Boolean)] = Seq(("38734", false)),
                   family_members_id: Seq[String] = Seq("38734"),
                   family_type_from_system: Option[String] = None
                 )
