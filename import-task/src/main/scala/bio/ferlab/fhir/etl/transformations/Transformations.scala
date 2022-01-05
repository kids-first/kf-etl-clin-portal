package bio.ferlab.fhir.etl.transformations

import bio.ferlab.datalake.spark3.transformation.{Custom, Drop, Transformation}
import bio.ferlab.fhir.etl._
import bio.ferlab.fhir.etl.Utils.{codingClassify, extractAclFromList, extractHashes, extractStudyExternalId, extractStudyVersion, firstNonNull, ncitIdAnatomicalSite, retrieveIsControlledAccess, retrieveIsHarmonized, retrieveRepository, retrieveSize, uberonIdAnatomicalSite}
import org.apache.spark.sql.functions.{col, collect_list, explode, expr, filter, regexp_extract, struct}
import org.apache.spark.sql.functions._

object Transformations {

  val URL_FILE_SIZE = "https://nih-ncpi.github.io/ncpi-fhir-ig/StructureDefinition/file-size"
  val URL_HASHES = "https://nih-ncpi.github.io/ncpi-fhir-ig/StructureDefinition/hashes"
  val patternParticipantStudy = "[A-Z][a-z]+-(SD_[0-9A-Za-z]+)-([A-Z]{2}_[0-9A-Za-z]+)"
  val participantSpecimen = "[A-Z][a-z]+/([0-9A-Za-z]+)"
  val conditionTypeR = "^https:\\/\\/[A-Za-z0-9-_.\\/]+\\/([A-Za-z0-9]+)"

  val patientMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      // TODO affected_status
      // TODO alias_group
      // TODO created_at
      .withColumn("ethnicity", col("ethnicity.text"))
      .withColumn("external_id", col("identifier")(0)("value"))
      // TODO is_proband
      .withColumn("participant_id", regexp_extract(col("identifier")(2)("value"), patternParticipantStudy, 2))
      // TODO modified_at
      .withColumn("race", col("race.text"))
      .withColumn("study_id", regexp_extract(col("identifier")(2)("value"), patternParticipantStudy, 1))
      // TODO visible
    ),
    Drop("extension", "id", "identifier", "meta")
  )

  val specimenMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      // TODO age_at_event_days
      // TODO analyte_type
      .withColumn("composition", col("type")("text"))
      // TODO concentration_mg_per_ml
      // TODO consent_type
      // TODO created_at
      // TODO duo_ids
      // TODO dbgap_consent_code
      // TODO external_aliquot_id
      // TODO external_sample_id
      .withColumn("specimen_id", col("identifier")(0)("value"))
      .withColumn("method_of_sample_procurement", col("collection")("method")("coding"))
      // TODO modified_at
      .withColumn("ncit_id_anatomical_site", ncitIdAnatomicalSite(col("collection")("bodySite")("coding")))
      // TODO ncit_id_tissue_type
      // TODO shipment_date
      // TODO shipment_origin
      .withColumn("participant_fhir_id",   regexp_extract(col("subject")("reference"), participantSpecimen, 1))
      // TODO source_text_tumor_descriptor
      // TODO source_text_tissue_type
      .withColumn("source_text_anatomical_site", col("collection")("bodySite")("text"))
      // TODO spatial_descriptor
      .withColumn("uberon_id_anatomical_site", uberonIdAnatomicalSite(col("collection")("bodySite")("coding")))
      .withColumn("volume_ul", col("collection")("quantity")("value"))
      .withColumn("volume_ul_unit", col("collection")("quantity")("unit"))
      // TODO visible
      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
    ),
    Drop()
  )

  val observationMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .withColumn("participant_fhir_id", regexp_extract( col("subject")("reference"), participantSpecimen, 1))
      .withColumn("vital_status", col("valueCodeableConcept")("text"))
      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
      .withColumn("observation_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
    ),
    Drop("extension", "id", "identifier", "meta")
  )

  val conditionMappings: List[Transformation] = List(
    Custom(_
      .select("*")
            .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
            .withColumn("diagnosis_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
            .withColumn("condition_coding", codingClassify(col("code")("coding")).cast("array<struct<category:string,code:string>>"))
            .withColumn("source_text", col("code")("text"))
            .withColumn("participant_fhir_id", regexp_extract( col("subject")("reference"), participantSpecimen, 1))
            //could be phenotype OR disease per condition
            .withColumn("observed", col("verificationStatus")("text"))
            .withColumn("source_text_tumor_location", col("bodySite")("text"))
            .withColumn("uberon_id_tumor_location", col("bodySite")("coding"))
            .withColumn("condition_profile", regexp_extract(col("meta")("profile")(0), conditionTypeR, 1))
      //      .withColumn("snomed_id_phenotype", col("code")) //TODO
      //      .withColumn("external_id", col("identifier")) //TODO
      //      .withColumn("diagnosis_category", col("code")) //TODO
    ),
    Drop("extension", "id", "identifier", "meta")
  )

  val organizationMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .withColumn("organization_id", col("identifier")(0)("value"))
    ),
    Drop("extension", "id", "identifier", "meta")
  )

  val researchstudyMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .withColumn("attribution", filter(col("identifier"), c => c("system") === SYS_NCBI_URL)(0)("value"))
      // TODO created_at
      // TODO data_access_authority
      .withColumn("external_id", extractStudyExternalId(filter(col("identifier"), c => c("system") === SYS_NCBI_URL)(0)("value")))
      // TODO modified_at
      .withColumnRenamed("title", "name")
      .withColumnRenamed("status", "release_status")
      .withColumn("version", extractStudyVersion(filter(col("identifier"), c => c("system") === SYS_NCBI_URL)(0)("value")))
      // TODO short_name
      // TODO code
      // TODO domain
      // TODO program
      // TODO visible
      .withColumn("study_id", filter(col("identifier"), c => c("system") === s"${SYS_DATASERVICE_URL}studies/")(0)("value"))
    ),
    Drop("extension", "id", "identifier", "meta")
  )

  val documentreferenceMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .withColumn("acl", extractAclFromList(col("securityLabel")("text")))
      .withColumn("access_urls", col("content")("attachment")("url")(0))
      // TODO availability
      .withColumn("controlled_access", retrieveIsControlledAccess(col("securityLabel")(0)("coding")(0)("code")))
      // TODO created_at
      .withColumn("data_type", col("type")("text"))
      .withColumn("external_id", col("content")(1)("attachment")("url"))
      .withColumn("file_format", firstNonNull(col("content")("format")("display")))
      .withColumn("file_name", firstNonNull(col("content")("attachment")("title")))
      .withColumn("genomic_file_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
      .withColumn("hashes", extractHashes(filter(col("content")(1)("attachment")("extension"), c => c("url") === URL_HASHES)("valueCodeableConcept")))
      // TODO instrument_models
      .withColumn("is_harmonized", retrieveIsHarmonized(col("content")(1)("attachment")("url")))
      // TODO is_paired_end
      .withColumn("latest_did", split(col("content")("attachment")("url")(0), "\\/\\/")(2))
      // TODO metadata
      // TODO modified_at
      // TODO platforms
      // TODO reference_genome
      .withColumn("repository", retrieveRepository(col("content")("attachment")("url")(0)))
      .withColumn("size", retrieveSize(filter(col("content")(1)("attachment")("extension"), c => c("url") === URL_FILE_SIZE)("valueDecimal")(0)))
      .withColumn("urls", col("content")(1)("attachment")("url"))
      // TODO visible
      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
      .withColumn("participant_fhir_id", regexp_extract( col("subject")("reference"), participantSpecimen, 1))
    ),
    Drop("extension", "id", "implicitRules", "language", "text", "meta", "contained",
      "status", "docStatus", "type", "category", "subject", "date", "author", "authenticator",
      "custodian", "relatesTo", "description", "masterIdentifier", "identifier", "securityLabel", "content", "context")
  )

  val groupMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
      .withColumn("family_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
      .withColumn("exploded_member", explode(col("member")))
      .withColumn("exploded_member_entity", regexp_extract(col("exploded_member")("entity")("reference"), participantSpecimen,1))
      .withColumn("exploded_member_inactive", col("exploded_member")("inactive"))
      .withColumn("family_members", struct("exploded_member_entity", "exploded_member_inactive"))
      .groupBy("fhir_id", "study_id", "family_id")
      .agg(
        collect_list("family_members") as "family_members"
      )
    ),
    Drop("extension", "id", "identifier", "meta")
  )

  val extractionMappings: Map[String, List[Transformation]] = Map(
    "patient" -> patientMappings,
    "specimen" -> specimenMappings,
    "observation" -> observationMappings,
    "condition" -> conditionMappings,
    "researchsubject" -> patientMappings,
    "researchstudy" -> researchstudyMappings,
    "group" -> groupMappings,
    "documentreference" -> documentreferenceMappings,
    "organization" -> organizationMappings,
  )

}
