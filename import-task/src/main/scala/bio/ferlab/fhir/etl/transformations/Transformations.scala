package bio.ferlab.fhir.etl.transformations

import bio.ferlab.datalake.spark3.transformation.{Custom, Drop, Transformation}
import bio.ferlab.fhir.etl._
import bio.ferlab.fhir.etl.Utils._
import org.apache.spark.sql.functions.{col, collect_list, explode, filter, regexp_extract, struct}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType

object Transformations {

  val patternParticipantStudy = "[A-Z][a-z]+-(SD_[0-9A-Za-z]+)-([A-Z]{2}_[0-9A-Za-z]+)"
  val patternPractitionerRoleResearchStudy = "PractitionerRole\\/([0-9]+)"
  val participantSpecimen = "[A-Z][a-z]+/([0-9A-Za-z]+)"
  val conditionTypeR = "^https:\\/\\/[A-Za-z0-9-_.\\/]+\\/([A-Za-z0-9]+)"

  val patientMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id","gender", "ethnicity", "identifier", "race")
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
    Drop("identifier")
  )

  val researchSubjectMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id", "identifier")
      .withColumn("external_id", col("identifier")(0)("value"))
      .withColumn("participant_id", regexp_extract(col("identifier")(2)("value"), patternParticipantStudy, 2))
      .withColumn("study_id", regexp_extract(col("identifier")(2)("value"), patternParticipantStudy, 1))
    ),
    Drop("identifier")
  )

  val specimenMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id", "type", "identifier", "collection", "subject", "status")
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
    Drop("type", "identifier", "collection", "subject")
  )

  val observationVitalStatusMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id","subject", "valueCodeableConcept", "identifier", "_effectiveDateTime")
      .withColumn("participant_fhir_id", regexp_extract( col("subject")("reference"), participantSpecimen, 1))
      .withColumn("vital_status", col("valueCodeableConcept")("text"))
      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
      .withColumn("observation_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
      .withColumn("age_at_event_days", struct(
        col("_effectiveDateTime")("effectiveDateTime")("offset")("value") as "value",
        col("_effectiveDateTime")("effectiveDateTime")("offset")("unit") as "units",
        filter(col("_effectiveDateTime")("effectiveDateTime")("event")("coding"), c => c("system") === "http://snomed.info/sct")(0)("display") as "from"
      ))
      // TODO external_id
    ),
    Drop("subject", "valueCodeableConcept", "identifier", "_effectiveDateTime")
  )

  val observationFamilyRelationshipMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id", "subject", "identifier", "focus", "valueCodeableConcept")
      .withColumn("participant_fhir_id", regexp_extract( col("subject")("reference"), participantSpecimen, 1))
      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
      .withColumn("observation_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
      .withColumn("participant1_id", col("subject")("reference"))
      .withColumn("participant2_id", col("focus")(0)("reference"))
      .withColumn(
        "participant1_to_participant_2_relationship",
        filter(col("valueCodeableConcept")("coding"), c => c("system") === ROLE_CODE_URL)(0)("display")
      )
      // TODO external_id

    ),
    Drop("subject", "identifier", "focus", "valueCodeableConcept")
  )

  val conditionDiseaseMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id","identifier", "code", "bodySite", "subject", "verificationStatus", "_recordedDate")
      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
      .withColumn("diagnosis_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
      .withColumn("condition_coding", codingClassify(col("code")("coding")).cast("array<struct<category:string,code:string>>"))
      .withColumn("source_text", col("code")("text"))
      .withColumn("participant_fhir_id", regexp_extract( col("subject")("reference"), participantSpecimen, 1))
      .withColumn("source_text_tumor_location", col("bodySite")("text"))
      .withColumn("uberon_id_tumor_location", col("bodySite")("coding"))
      .withColumn("affected_status", col("verificationStatus")("text").cast(BooleanType))
      .withColumn("affected_status_text", col("verificationStatus")("coding")("display")(0))
      .withColumn("age_at_event", struct(
        col("_recordedDate")("recordedDate")("offset")("value") as "value",
        col("_recordedDate")("recordedDate")("offset")("unit") as "units",
        filter(col("_recordedDate")("recordedDate")("event")("coding"), c => c("system") === "http://snomed.info/sct")(0)("display") as "from"
      ))
      // TODO external_id
      // TODO diagnosis_category
    ),
    Drop("identifier", "code", "subject", "verificationStatus", "_recordedDate")
  )

  val conditionPhenotypeMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id","identifier", "code", "subject", "verificationStatus", "_recordedDate")
      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
      .withColumn("phenotype_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
      .withColumn("condition_coding", codingClassify(col("code")("coding")).cast("array<struct<category:string,code:string>>"))
      .withColumn("source_text", col("code")("text"))
      .withColumn("participant_fhir_id", regexp_extract( col("subject")("reference"), participantSpecimen, 1))
      .withColumn("observed", col("verificationStatus")("text"))
      .withColumn("age_at_event", struct(
        col("_recordedDate")("recordedDate")("offset")("value") as "value",
        col("_recordedDate")("recordedDate")("offset")("unit") as "units",
        filter(col("_recordedDate")("recordedDate")("event")("coding"), c => c("system") === "http://snomed.info/sct")(0)("display") as "from"
      ))
      // TODO snomed_id_phenotype
      // TODO external_id
    ),
    Drop("identifier", "code", "subject", "verificationStatus", "_recordedDate")
  )

  val organizationMappings: List[Transformation] = List(
    Custom(_
      .select( "fhir_id", "release_id","identifier", "name")
      .withColumn("organization_id", col("identifier")(0)("value"))
      .withColumn("institution", col("name"))
    ),
    Drop("identifier", "name")
  )

  val researchstudyMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id","title", "identifier", "principalInvestigator", "status")
      .withColumn("attribution", filter(col("identifier"), c => c("system") === SYS_NCBI_URL)(0)("value"))
      // TODO created_at
      // TODO data_access_authority
      .withColumn("external_id", extractStudyExternalId(filter(col("identifier"), c => c("system") === SYS_NCBI_URL)(0)("value")))
      // TODO modified_at
      .withColumnRenamed("title", "name")
      .withColumn("version", extractStudyVersion(filter(col("identifier"), c => c("system") === SYS_NCBI_URL)(0)("value")))
      .withColumn(
        "investigator_id",
        regexp_extract(col("principalInvestigator")("reference"), patternPractitionerRoleResearchStudy, 1)
      )
      // TODO short_name
      // TODO code
      // TODO domain
      // TODO program
      // TODO visible
      .withColumn("study_id", filter(col("identifier"), c => c("system") === s"${SYS_DATASERVICE_URL}studies/")(0)("value"))
    ),
    Drop("title", "identifier", "principalInvestigator")
  )

  val documentreferenceMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id", "securityLabel", "content", "type", "identifier", "subject")
      .withColumn("acl", extractAclFromList(col("securityLabel")("text")))
      .withColumn("access_urls", col("content")("attachment")("url")(0))
      // TODO availability
      .withColumn("access", retrieveIsControlledAccess(col("securityLabel")(0)("coding")(0)("code")))
      // TODO created_at
      .withColumn("data_type", col("type")("text"))
      .withColumn("external_id", col("content")(1)("attachment")("url"))
      .withColumn("file_format", firstNonNull(col("content")("format")("display")))
      .withColumn("file_name", firstNonNull(col("content")("attachment")("title")))
      .withColumn("file_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
      .withColumn("hashes", extractHashes(col("content")(1)("attachment")("hashes")))
      // TODO instrument_models
      .withColumn("is_harmonized", retrieveIsHarmonized(col("content")(1)("attachment")("url")))
      // TODO is_paired_end
      .withColumn("latest_did", split(col("content")("attachment")("url")(0), "\\/\\/")(2))
      // TODO metadata
      // TODO modified_at
      // TODO platforms
      // TODO reference_genome
      .withColumn("repository", retrieveRepository(col("content")("attachment")("url")(0)))
      .withColumn("size", retrieveSize(col("content")(1)("attachment")("fileSize")))
      .withColumn("urls", col("content")(1)("attachment")("url"))
      // TODO visible
      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
      .withColumn("participant_fhir_id", regexp_extract( col("subject")("reference"), participantSpecimen, 1))
    ),
    Drop("securityLabel", "content", "type", "identifier", "subject")
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
      .groupBy("fhir_id", "study_id", "family_id", "type", "release_id")
      .agg(
        collect_list("family_members") as "family_members",
        collect_list("exploded_member_entity") as "family_members_id"
      )
    ),
    Drop()
  )

  val extractionMappings: Map[String, List[Transformation]] = Map(
    "patient" -> patientMappings,
    "specimen" -> specimenMappings,
    "observation_vital-status" -> observationVitalStatusMappings,
    "observation_family-relationship" -> observationFamilyRelationshipMappings,
    "condition_phenotype" -> conditionPhenotypeMappings,
    "condition_disease" -> conditionDiseaseMappings,
    "researchsubject" -> researchSubjectMappings,
    "researchstudy" -> researchstudyMappings,
    "group" -> groupMappings,
    "documentreference" -> documentreferenceMappings,
    "organization" -> organizationMappings,
  )

}
