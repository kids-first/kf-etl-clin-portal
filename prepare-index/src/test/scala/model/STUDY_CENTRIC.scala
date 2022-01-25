package model

case class STUDY_CENTRIC(
                          `fhir_id`: String = "42776",
                          `study_name`: String = "Kids First: Genomic Analysis of Congenital Heart Defects and Acute Lymphoblastic Leukemia in Children with Down Syndrome",
                          `status`: String = "completed",
                          `attribution`: String = "phs002330.v1.p1",
                          `external_id`: String = "phs002330",
                          `version`: String = "v1.p1",
                          `investigator_id`: String = "123456",
                          `study_id`: String = "SD_Z6MWD3H0",
                          `study_code`: String = "TODO",
                          `program`: String = "TODO",
                          `type_of_omics`: Seq[String] = Seq("TODO"),
                          `experimental_strategy`: Seq[String] = Seq("TODO"),
                          `data_access`: Seq[String] = Seq("TODO"),
                          `participant_count`: Long = 0,
                          `file_count`: Long = 0,
                          `family_count`: Long = 0,
                          `family_data`: Boolean = false,
                          `release_id`: String = "re_000001"
                        )
