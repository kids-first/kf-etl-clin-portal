package model

case class LIGHT_STUDY_CENTRIC (
                           `fhir_id`: String = "42776",
                           `study_name`: String = "Kids First: Genomic Analysis of Congenital Heart Defects and Acute Lymphoblastic Leukemia in Children with Down Syndrome",
                           `status`: String = "completed",
                           `attribution`: String = "phs002330.v1.p1",
                           `external_id`: String = "phs002330",
                           `version`: String = "v1.p1",
                           `investigator_id`: String = "123456",
                           `study_id`: String = "SD_Z6MWD3H0",
                           `study_code`: String = "KF-CHDALL",
                           `program`: String = "Kids First",
                           data_category: Seq[String] = Seq("Genomics"),
                           `experimental_strategy`: Seq[String] = Seq("WGS"),
                           `controlled_access`: Seq[String] = Seq("Controlled"),
                           `participant_count`: Long = 0,
                           `file_count`: Long = 0,
                           `family_count`: Long = 0,
                           `family_data`: Boolean = false,
                           `biospecimen_count`: Long = 0,
                         )

case class STUDY_CENTRIC (
                           `fhir_id`: String = "42776",
                           `study_name`: String = "Kids First: Genomic Analysis of Congenital Heart Defects and Acute Lymphoblastic Leukemia in Children with Down Syndrome",
                           `status`: String = "completed",
                           `attribution`: String = "phs002330.v1.p1",
                           `external_id`: String = "phs002330",
                           `version`: String = "v1.p1",
                           `investigator_id`: String = "123456",
                           `study_id`: String = "SD_Z6MWD3H0",
                           `study_code`: String = "KF-CHDALL",
                           `program`: String = "Kids First",
                           data_category: Seq[String] = Seq("Genomics"),
                           `experimental_strategy`: Seq[String] = Seq("WGS"),
                           `controlled_access`: Seq[String] = Seq("Controlled"),
                           `participant_count`: Long = 0,
                           `file_count`: Long = 0,
                           `family_count`: Long = 0,
                           `family_data`: Boolean = false,
                           `biospecimen_count`: Long = 0,
                           `release_id`: String = "re_000001"
                               )
