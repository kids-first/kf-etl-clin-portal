package model

case class DOCUMENTREFERENCE(
                 `fhir_id`: String = "337786",
                 `acl`: Seq[String] = Seq("phs002330.c999", "SD_Z6MWD3H0", "phs002330.c1"),
                 `access_urls`: String = "drs://data.kidsfirstdrc.org//a4e15e78-de88-44d8-87f4-7f56cda2475f",
                 `access`: String = "Controlled",
                 `data_type`: String = "gVCF",
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
                 `participant_fhir_id`: String = "39167"
               )
