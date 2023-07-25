package bio.ferlab.etl.testmodels.prepared



case class PREPARED_PARTICIPANT(
                                 `fhir_id`: String = "38734",
                                 `participant_facet_ids`: PARTICIPANT_FACET_IDS = PARTICIPANT_FACET_IDS(),
                                 `sex`: String = "male",
                                 `ethnicity`: String = "Not Reported",
                                 `race`: String = "Not Reported",
                                 `external_id`: String = "PAVKKD",
                                 `participant_id`: String = "PT_48DYT4PP",
                                 `study_id`: String = "SD_Z6MWD3H0",
                                 `release_id`: String = "re_000001",
                                 `phenotype`: Seq[PREPARED_PHENOTYPE] = Seq.empty,
                                 `observed_phenotype`: Seq[PREPARED_ONTOLOGY_TERM] = Seq.empty,
                                 `non_observed_phenotype`: Seq[PREPARED_ONTOLOGY_TERM] = Seq.empty,
                                 `diagnosis`: Seq[PREPARED_DIAGNOSIS] = Seq.empty,
                                 `mondo`: Seq[PREPARED_ONTOLOGY_TERM] = Seq.empty,
                                 `outcomes`: Seq[PREPARED_OUTCOME] = Seq.empty,
                                 `family`: PREPARED_FAMILY = null,
                                 `family_type`: String = "probant_only",
                                 `down_syndrome_status`: String = "D21",
                                 `down_syndrome_diagnosis`: Seq[String] = null,
                                 `is_proband`: Boolean = false, // TODO
                                 `age_at_data_collection`: Int = 111, // TODO
                                 `study`: PREPARED_LIGHT_STUDY = PREPARED_LIGHT_STUDY(),
                                 `files`: Seq[FILE_WITH_BIOSPECIMEN] = Seq.empty,
                                 `study_external_id`: String = "phs002330",
                                 `nb_files`: Long = 0,
                                 `nb_biospecimens`: Long = 0
                              )

case class PARTICIPANT_FACET_IDS(
                                 participant_fhir_id_1: String = "38734",
                                 participant_fhir_id_2: String = "38734"
                               )
case class FILE_WITH_BIOSPECIMEN(
                                  `fhir_id`: Option[String] = Some("337786"),
                                  `file_facet_ids`: FILE_WITH_BIOSPECIMEN_FACET_IDS = FILE_WITH_BIOSPECIMEN_FACET_IDS(),
                                  `acl`: Option[Seq[String]] = Some(Seq("phs002330.c999", "SD_Z6MWD3H0", "phs002330.c1")),
                                  `access_urls`: Option[String] = Some("drs://data.kidsfirstdrc.org//a4e15e78-de88-44d8-87f4-7f56cda2475f"),
                                  `controlled_access`: Option[String] = Some("Controlled"),
                                  `data_type`: Option[String] = Some("gVCF"),
                                  `external_id`: Option[String] = Some("s3://kf-study-us-east-1-prd-sd-z6mwd3h0/harmonized-data/raw-gvcf/4db9adf4-94f7-4800-a360-49eda89dfb62.g.vcf.gz"),
                                  `file_format`: Option[String] = Some("gvcf"),
                                  `file_name`: Option[String] = Some("4db9adf4-94f7-4800-a360-49eda89dfb62.g.vcf.gz"),
                                  `file_id`: Option[String] = Some("GF_067MR115"),
                                  `hashes`: Option[Map[String, String]] = Some(Map("md5" -> "3f06b25fb517e3c4af688e5539f3a67b")),
                                  `is_harmonized`: Option[Boolean] = Some(true),
                                  `latest_did`: Option[String] = Some("a4e15e78-de88-44d8-87f4-7f56cda2475f"),
                                  `repository`: Option[String] = Some("gen3"),
                                  `size`: Option[BigInt] = Some(BigInt.apply(2610321004L)),
                                  `urls`: Option[String] = Some("s3://kf-study-us-east-1-prd-sd-z6mwd3h0/harmonized-data/raw-gvcf/4db9adf4-94f7-4800-a360-49eda89dfb62.g.vcf.gz"),
                                  `study_id`: Option[String] = Some("SD_Z6MWD3H0"),
                                  `release_id`: Option[String] = Some("re_000001"),
                                  `biospecimens`: Seq[PREPARED_BIOSPECIMEN_FOR_FILE] = Seq.empty,
                                  `sequencing_experiment`: Seq[PREPARED_SEQUENCING_EXPERIMENT] = Seq(PREPARED_SEQUENCING_EXPERIMENT())
                                )

case class FILE_WITH_BIOSPECIMEN_FACET_IDS(
                                            file_fhir_id_1: Option[String] = Some("337786"),
                                            file_fhir_id_2: Option[String] = Some("337786")
                                          )