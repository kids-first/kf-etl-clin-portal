package bio.ferlab.fhir.etl.transformations

import bio.ferlab.datalake.spark3.transformation.{Custom, Drop, Transformation}
import bio.ferlab.fhir.etl.Utils._
import bio.ferlab.fhir.etl._
import org.apache.spark.sql.functions.{col, collect_list, explode, filter, regexp_extract, struct, _}
import org.apache.spark.sql.types.BooleanType

object Transformations {

  val patternPractitionerRoleResearchStudy = "PractitionerRole\\/([0-9]+)"
  val conditionTypeR = "^https:\\/\\/[A-Za-z0-9-_.\\/]+\\/([A-Za-z0-9]+)"

  val patientMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "study_id", "release_id","gender", "ethnicity", "identifier", "race")
      .withColumn("ethnicity", col("ethnicity.text"))
      .withColumn("external_id", filter(col("identifier"), c => c("system").isNull)(0)("value"))
      // TODO is_proband
      .withColumn("participant_id", extractFirstForSystem(col("identifier"), SYSTEM_URL)("value"))
      .withColumn("race", col("race.text"))
    ),
    Drop("identifier")
  )

  val researchSubjectMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id", "identifier", "study_id")
      .withColumn("external_id", filter(col("identifier"), c => c("system").isNull)(0)("value"))
      .withColumn("participant_id", extractFirstForSystem(col("identifier"), SYSTEM_URL)("value"))
    ),
    Drop("identifier")
  )

  val specimenMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id", "study_id", "type", "identifier", "collection", "subject", "status")
      // TODO age_at_event_days
      // TODO analyte_type
      .withColumn("composition", col("type")("text"))
      // TODO concentration_mg_per_ml
      // TODO consent_type
      // TODO created_at
      // TODO duo_ids
      // TODO dbgap_consent_code
      // TODO external_sample_id
      .withColumn("specimen_id", extractFirstForSystem(col("identifier"), SYSTEM_URL)("value"))
      .withColumn("external_aliquot_id", filter(col("identifier"), c => c("system").isNull)(0)("value"))
      .withColumn("method_of_sample_procurement", col("collection")("method")("coding"))
      // TODO modified_at
      .withColumn("ncit_id_anatomical_site", ncitIdAnatomicalSite(col("collection")("bodySite")("coding")))
      // TODO ncit_id_tissue_type
      // TODO shipment_date
      // TODO shipment_origin
      .withColumn("participant_fhir_id",   extractReferenceId(col("subject")("reference")))
      // TODO source_text_tumor_descriptor
      // TODO source_text_tissue_type
      .withColumn("source_text_anatomical_site", col("collection")("bodySite")("text"))
      // TODO spatial_descriptor
      .withColumn("uberon_id_anatomical_site", uberonIdAnatomicalSite(col("collection")("bodySite")("coding")))
      .withColumn("volume_ul", col("collection")("quantity")("value"))
      .withColumn("volume_ul_unit", col("collection")("quantity")("unit"))
      // TODO visible
    ),
    Drop("type", "identifier", "collection", "subject")
  )

  val observationVitalStatusMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id","subject", "valueCodeableConcept", "identifier", "_effectiveDateTime", "study_id")
      .withColumn("participant_fhir_id", extractReferenceId( col("subject")("reference")))
      .withColumn("vital_status", col("valueCodeableConcept")("text"))
      .withColumn("observation_id", extractFirstForSystem(col("identifier"), SYSTEM_URL)("value"))
      .withColumn("age_at_event_days", struct(
        col("_effectiveDateTime")("effectiveDateTime")("offset")("value") as "value",
        col("_effectiveDateTime")("effectiveDateTime")("offset")("unit") as "units",
        extractFirstForSystem(col("_effectiveDateTime")("effectiveDateTime")("event")("coding"), Seq("http://snomed.info/sct"))("display") as "from"
      ))
      // TODO external_id
    ),
    Drop("subject", "valueCodeableConcept", "identifier", "_effectiveDateTime")
  )

  val observationFamilyRelationshipMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id", "subject", "identifier", "focus", "valueCodeableConcept", "meta")
      .withColumn("study_id", col("meta")("tag")(0)("code"))
      .withColumn("observation_id", extractFirstForSystem(col("identifier"), SYSTEM_URL)("value"))
      .withColumn("participant1_fhir_id", extractReferenceId( col("subject")("reference")))
      .withColumn("participant2_fhir_id", extractReferencesId(col("focus")("reference"))(0))
      .withColumn(
        "participant1_to_participant_2_relationship",
        extractFirstForSystem(col("valueCodeableConcept")("coding"), ROLE_CODE_URL)("display")
      )
      // TODO external_id
    ),
    Drop("subject", "identifier", "focus", "valueCodeableConcept", "meta")
  )

  val conditionDiseaseMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id","study_id", "release_id","identifier", "code", "bodySite", "subject", "verificationStatus", "_recordedDate")
      .withColumn("diagnosis_id", extractFirstForSystem(col("identifier"), SYSTEM_URL)("value"))
      .withColumn("condition_coding", codingClassify(col("code")("coding")).cast("array<struct<category:string,code:string>>"))
      .withColumn("source_text", col("code")("text"))
      .withColumn("participant_fhir_id", extractReferenceId( col("subject")("reference")))
      .withColumn("source_text_tumor_location", col("bodySite")("text"))
      .withColumn("uberon_id_tumor_location", flatten(transform(col("bodySite")("coding"), c => c("display"))))
      .withColumn("affected_status", col("verificationStatus")("text").cast(BooleanType))
      .withColumn("affected_status_text", col("verificationStatus")("coding")("display")(0))
      .withColumn("down_syndrome_diagnosis", col("verificationStatus")("text"))
      .withColumn("age_at_event", struct(
        col("_recordedDate")("recordedDate")("offset")("value") as "value",
        col("_recordedDate")("recordedDate")("offset")("unit") as "units",
        extractFirstForSystem(col("_recordedDate")("recordedDate")("event")("coding"), Seq("http://snomed.info/sct"))("display") as "from"
      ))
      // TODO external_id
      // TODO diagnosis_category
    ),
    Drop("identifier", "code", "subject", "verificationStatus", "_recordedDate", "bodySite")
  )

  val conditionPhenotypeMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "study_id", "release_id","identifier", "code", "subject", "verificationStatus", "_recordedDate")
      .withColumn("phenotype_id", extractFirstForSystem(col("identifier"), SYSTEM_URL)("value"))
      .withColumn("condition_coding", codingClassify(col("code")("coding")).cast("array<struct<category:string,code:string>>"))
      .withColumn("source_text", col("code")("text"))
      .withColumn("participant_fhir_id", extractReferenceId( col("subject")("reference")))
      .withColumn("observed", col("verificationStatus")("text"))
      .withColumn("age_at_event", struct(
        col("_recordedDate")("recordedDate")("offset")("value") as "value",
        col("_recordedDate")("recordedDate")("offset")("unit") as "units",
        //todo same for include?
        extractFirstForSystem(col("_recordedDate")("recordedDate")("event")("coding"), Seq("http://snomed.info/sct"))("display") as "from"
      ))
      // TODO snomed_id_phenotype
      // TODO external_id
    ),
    Drop("identifier", "code", "subject", "verificationStatus", "_recordedDate")
  )

  val organizationMappings: List[Transformation] = List(
    Custom(_
      .select( "fhir_id", "release_id", "study_id","identifier", "name")
      .withColumn("organization_id", extractFirstForSystem(col("identifier"), SYSTEM_URL)("value"))
      .withColumn("institution", col("name"))
    ),
    Drop("identifier", "name")
  )

  val taskMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "study_id", "release_id", "output", "input", "identifier")
      .withColumn("task_id", extractFirstForSystem(col("identifier"), SYSTEM_URL)("value"))
      .withColumn(
        "document_reference_fhir_ids", transform(
          filter(col("output"), c => c("type")("text") === "genomic_file"),
          c => regexp_extract(c("valueReference")("reference"), documentReferenceExtract, 1)
        )
      )
      .withColumn(
        "biospecimen_fhir_ids", transform(
          filter(col("input"), c => c("type")("text") === "biospecimen"),
          c => regexp_extract(c("valueReference")("reference"), specimenExtract, 1)
        )
      )
      .withColumn(
        "experiment_strategy", transform(
          filter(col("input"), c => c("type")("text") === "experiment_strategy"), c => c("valueString")
        )(0)
      )
      .withColumn(
        "instrument_model", transform(
          filter(col("input"), c => c("type")("text") === "instrument_model"), c => c("valueString")
        )(0)
      )
      .withColumn(
        "library_name", transform(
          filter(col("input"), c => c("type")("text") === "library_name"), c => c("valueString")
        )(0)
      )
      .withColumn(
        "library_strand", transform(
          filter(col("input"), c => c("type")("text") === "library_strand"), c => c("valueString")
        )(0)
      )
      .withColumn(
        "platform", transform(
          filter(col("input"), c => c("type")("text") === "platform"), c => c("valueString")
        )(0)
      )
    ),
    Drop("output", "input", "identifier")
  )

  val researchstudyMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "keyword", "release_id", "study_id","title", "identifier", "principalInvestigator", "status")
      .withColumn("attribution", extractFirstForSystem(col("identifier"), Seq(SYS_NCBI_URL))("value"))
      // TODO data_access_authority
      .withColumn("external_id", extractStudyExternalId(extractFirstForSystem(col("identifier"), Seq(SYS_NCBI_URL))("value")))
      .withColumnRenamed("title", "name")
      .withColumn("version", extractStudyVersion(extractFirstForSystem(col("identifier"), Seq(SYS_NCBI_URL))("value")))
      .withColumn(
        "investigator_id",
        regexp_extract(col("principalInvestigator")("reference"), patternPractitionerRoleResearchStudy, 1)
      )
      // TODO short_name
      .withColumn("study_code", col("keyword")(1)("coding")(0)("code"))
      // TODO domain
      .withColumn("program", col("keyword")(0)("coding")(0)("code"))
    ),
    Drop("title", "identifier", "principalInvestigator", "keyword")
  )

  val documentreferenceMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id","study_id", "release_id", "securityLabel", "content", "type", "identifier", "subject", "context", "docStatus")
      .withColumn("access_urls", col("content")("attachment")("url")(0))
      // TODO availability
      .withColumn("acl", extractAclFromList(col("securityLabel")("text"), col("study_id")))
      .withColumn("controlled_access", col("securityLabel")(0)("text"))
      .withColumn("data_type", col("type")("text"))
      .withColumn("data_category", col("category")(0)("text"))
      .withColumn("external_id", col("content")(1)("attachment")("url"))
      .withColumn("file_format", firstNonNull(col("content")("format")("display")))
      .withColumn("file_name", firstNonNull(col("content")("attachment")("title")))
      .withColumn("file_id", extractFirstForSystem(col("identifier"), SYSTEM_URL)("value"))
      .withColumn("hashes", extractHashes(col("content")(1)("attachment")("hashes")))
      // TODO instrument_models
      .withColumn("is_harmonized", retrieveIsHarmonized(col("content")(1)("attachment")("url")))
      // TODO is_paired_end
      .withColumn("latest_did", split(col("content")("attachment")("url")(0), "\\/\\/")(2))
      // TODO platforms
      // TODO reference_genome
      .withColumn("repository", retrieveRepository(col("content")("attachment")("url")(0)))
      .withColumn("size", retrieveSize(col("content")(1)("attachment")("fileSize")))
      .withColumn("urls", col("content")(1)("attachment")("url"))
      .withColumn("participant_fhir_ids", array(extractReferenceId( col("subject")("reference"))))
      .withColumn("specimen_fhir_ids", extractReferencesId(col("context")("related")("reference")))
      .withColumnRenamed("docStatus", "status")
    ),
    Drop("securityLabel", "content", "type", "identifier", "subject", "context")
  )

  val groupMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .withColumn("external_id", filter(col("identifier"), c => c("system").isNull)(0)("value"))
      .withColumn("family_id", extractFirstForSystem(col("identifier"), SYSTEM_URL)("value"))
      .withColumn("exploded_member", explode(col("member")))
      .withColumn("exploded_member_entity", extractReferenceId(col("exploded_member")("entity")("reference")))
      .withColumn("exploded_member_inactive", col("exploded_member")("inactive"))
      .withColumn("family_members", struct("exploded_member_entity", "exploded_member_inactive"))
      .groupBy("fhir_id", "study_id", "family_id", "external_id", "type", "release_id")
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
    "vital_status" -> observationVitalStatusMappings,
    "family_relationship" -> observationFamilyRelationshipMappings,
    "phenotype" -> conditionPhenotypeMappings,
    "disease" -> conditionDiseaseMappings,
    "researchsubject" -> researchSubjectMappings,
    "researchstudy" -> researchstudyMappings,
    "group" -> groupMappings,
    "drs_document_reference" -> documentreferenceMappings,
    "organization" -> organizationMappings,
    "task" -> taskMappings,
  )

}
