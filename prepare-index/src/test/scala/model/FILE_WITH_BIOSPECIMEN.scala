package model

case class FILE_WITH_BIOSPECIMEN(
                                  `fhir_id`: Option[String] = Some("337786"),
                                  `file_fhir_id`: Option[String] = Some("337786"),
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
                                  `biospecimens`: Seq[BIOSPECIMEN] = Seq.empty,
                                  `sequencing_experiment`: Option[SEQUENCING_EXPERIMENT] = None
                                )
