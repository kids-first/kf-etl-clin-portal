package bio.ferlab.etl.testmodels.prepared

case class PREPARED_FILE(
                          fhir_id: String = "337786",
                          file_facet_ids: FILE_FACET_IDS = FILE_FACET_IDS(),
                          acl: Seq[String] = Seq("phs002330.c999", "SD_Z6MWD3H0", "phs002330.c1"),
                          access_urls: String = "drs://data.kidsfirstdrc.org//a4e15e78-de88-44d8-87f4-7f56cda2475f",
                          controlled_access: String = "Controlled",
                          data_type: String = "gVCF",
                          external_id: String = "s3://kf-study-us-east-1-prd-sd-z6mwd3h0/harmonized-data/raw-gvcf/4db9adf4-94f7-4800-a360-49eda89dfb62.g.vcf.gz",
                          file_format: String = "gvcf",
                          file_name: String = "4db9adf4-94f7-4800-a360-49eda89dfb62.g.vcf.gz",
                          file_id: String = "GF_067MR115",
                          hashes: Map[String, String] = Map("md5" -> "3f06b25fb517e3c4af688e5539f3a67b"),
                          is_harmonized: Boolean = true,
                          latest_did: String = "a4e15e78-de88-44d8-87f4-7f56cda2475f",
                          repository: String = "gen3",
                          size: BigInt = BigInt.apply(2610321004L),
                          urls: String = "s3://kf-study-us-east-1-prd-sd-z6mwd3h0/harmonized-data/raw-gvcf/4db9adf4-94f7-4800-a360-49eda89dfb62.g.vcf.gz",
                          study_id: String = "SD_Z6MWD3H0",
                          data_category: String = "Genomics",
                          study: PREPARED_LIGHT_STUDY = PREPARED_LIGHT_STUDY(),
                          release_id: String = "re_000001",
                          participants: Seq[PREPARED_FILE_PARTICIPANT] = Seq.empty,
                          sequencing_experiment: Seq[PREPARED_SEQUENCING_EXPERIMENT] = Seq(PREPARED_SEQUENCING_EXPERIMENT()),
                          nb_participants: Long,
                          nb_biospecimens: Long,
                          fhir_document_reference: String = "http://localhost:8080/DocumentReference?identifier=GF_067MR115"
                       )

case class FILE_FACET_IDS(
                           file_fhir_id_1: String = "337786",
                           file_fhir_id_2: String = "337786"
                         )

case class PREPARED_FILE_PARTICIPANT(
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
                                      `biospecimens`: Set[PREPARED_BIOSPECIMEN_FOR_FILE] = Set.empty
                                       )