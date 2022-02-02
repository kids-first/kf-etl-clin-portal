package model

case class GROUP(
                   `fhir_id`: String = "42367",
                   `study_id`: String = "SD_Z6MWD3H0",
                   `family_id`: String = "FM_NV901ZZN",
                   `family_members`: Seq[(String, Boolean)] = Seq(("38734", false)),
                   `family_members_id`: Seq[String] = Seq("38734")
                 )

case class FAMILY(
                           `fhir_id`: String = "42367",
                           `study_id`: String = "SD_Z6MWD3H0",
                           `family_id`: String = "FM_NV901ZZN",
                           `family_members`: Seq[(String, Boolean)] = Seq(("38734", false)),
                         )