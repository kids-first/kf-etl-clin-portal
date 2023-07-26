package bio.ferlab.etl.testmodels.prepared

case class PREPARED_BIOSPECIMEN(
                                 `fhir_id`: String = "336842",
                                 `biospecimen_facet_ids`: BIOSPECIMEN_FACET_IDS = BIOSPECIMEN_FACET_IDS(),
                                 `status`: String = "available",
                                 `composition`: String = "Not Reported",
                                 `participant_fhir_id`: String = "38986",
                                 `volume_ul`: Long = 11,
                                 `volume_ul_unit`: String = null,
                                 `study_id`: String = "SD_Z6MWD3H0",
                                 `release_id`: String = "re_000001",
                                 `study`: PREPARED_LIGHT_STUDY = PREPARED_LIGHT_STUDY(),
                                 `participant`: PREPARED_SIMPLE_PARTICIPANT = PREPARED_SIMPLE_PARTICIPANT(),
                                 `files`: Seq[PREPARED_FILES_FOR_BIOSPECIMEN] = Seq.empty,
                                 `nb_files`: Long,
                                 `diagnosis_mondo`: String = null,
                                 `diagnosis_ncit`: String = null,
                                 `diagnosis_icd`: Seq[String] = null,
                                 `source_text`: String = null,
                                 `source_text_tumor_location`: Seq[String] = null,
                              )

case class PREPARED_FILES_FOR_BIOSPECIMEN(
                              `fhir_id`: String = "337786",
                              `file_facet_ids`: FILE_FACET_IDS = FILE_FACET_IDS(),
                              `acl`: Seq[String] = Seq("phs002330.c999", "SD_Z6MWD3H0", "phs002330.c1"),
                              `access_urls`: String = "drs://data.kidsfirstdrc.org//a4e15e78-de88-44d8-87f4-7f56cda2475f",
                              `controlled_access`: String = "Controlled",
                              `data_type`: String = "gVCF",
                              `data_category`: String = "Genomics",
                              `external_id`: String = "s3://kf-study-us-east-1-prd-sd-z6mwd3h0/harmonized-data/raw-gvcf/4db9adf4-94f7-4800-a360-49eda89dfb62.g.vcf.gz",
                              `file_format`: String = "gvcf",
                              `file_name`: String = "4db9adf4-94f7-4800-a360-49eda89dfb62.g.vcf.gz",
                              `file_id`: String = "GF_067MR115",
                              `hashes`: Map[String, String] = Map("md5" -> "3f06b25fb517e3c4af688e5539f3a67b"),
                              `is_harmonized`: Boolean = true,
                              `latest_did`: String = "a4e15e78-de88-44d8-87f4-7f56cda2475f",
                              `repository`: String = "gen3",
                              `size`: BigInt = BigInt.apply(2610321004L),
                              `urls`: String = "s3://kf-study-us-east-1-prd-sd-z6mwd3h0/harmonized-data/raw-gvcf/4db9adf4-94f7-4800-a360-49eda89dfb62.g.vcf.gz",
                              `study_id`: String = "SD_Z6MWD3H0",
                              `release_id`: String = "re_000001",
                              `sequencing_experiment`: Seq[PREPARED_SEQUENCING_EXPERIMENT] = Seq(PREPARED_SEQUENCING_EXPERIMENT())
                            )