package bio.ferlab.etl.normalized.clinical

import bio.ferlab.datalake.spark3.transformation.{Custom, Drop, Transformation}
import bio.ferlab.etl.Utils.{firstCategory, observableTitleStandard}
import bio.ferlab.etl.normalized.clinical.Utils._
import bio.ferlab.etl.normalized.clinical.clinical._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType

object Transformations {

  val patientMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "study_id", "release_id", "gender", "ethnicity", "identifier", "race")
      .withColumn("external_id_from_no_system", filter(col("identifier"), c => c("system").isNull)(0)("value"))
      .withColumn("external_id_from_secondary_use", filter(col("identifier"), c => c("use") === "secondary")(0)("value"))
      .withColumn("external_id", coalesce(col("external_id_from_secondary_use"), col("external_id_from_no_system")))
      .withColumn("participant_id", officialIdentifier)
      .withColumn("race_omb", ombCategory(col("race.ombCategory")))
      .withColumn("ethnicity", ombCategory(col("ethnicity.ombCategory")))
      .withColumn("race", when(col("race_omb").isNull && lower(col("race.text")) === "more than one race", upperFirstLetter(col("race.text"))).otherwise(col("race_omb")))
    ),
    Drop("identifier", "race_omb", "external_id_from_no_system", "external_id_from_secondary_use")
  )

  val researchSubjectMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id", "identifier", "study_id")
      .withColumn("external_id", filter(col("identifier"), c => c("system").isNull)(0)("value"))
      .withColumn("participant_id", officialIdentifier)
    ),
    Drop("identifier")
  )

  val observationVitalStatusMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id", "subject", "valueCodeableConcept", "identifier", "_effectiveDateTime", "study_id")
      .withColumn("participant_fhir_id", extractReferenceId(col("subject")("reference")))
      .withColumn("vital_status", col("valueCodeableConcept")("text"))
      .withColumn("observation_id", officialIdentifier)
      .withColumn("age_at_event_days", struct(
        col("_effectiveDateTime")("effectiveDateTime")("offset")("value") as "value",
        col("_effectiveDateTime")("effectiveDateTime")("offset")("unit") as "units",
      ))
    ),
    Drop("subject", "valueCodeableConcept", "identifier", "_effectiveDateTime", "parent_0")
  )

  val observationFamilyRelationshipMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id", "subject", "identifier", "focus", "valueCodeableConcept", "meta")
      .withColumn("study_id", col("meta")("tag")(0)("code"))
      .withColumn("observation_id", officialIdentifier)
      .withColumn("participant1_fhir_id", extractReferenceId(col("subject")("reference")))
      .withColumn("participant2_fhir_id", extractReferencesId(col("focus")("reference"))(0))
      .withColumn(
        "participant1_to_participant_2_relationship",
        extractFirstForSystem(col("valueCodeableConcept")("coding"), ROLE_CODE_URL)("display")
      )
      // TODO external_id
    ),
    Drop("subject", "identifier", "focus", "valueCodeableConcept", "meta")
  )

  private val extractThenReformatConditionCoding = (category: String) => observableTitleStandard(firstCategory(category, col("condition_coding")))

  val diseaseMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "study_id", "release_id", "identifier", "code", "bodySite", "subject", "verificationStatus", "_recordedDate", "category")
      .withColumn("diagnosis_id", officialIdentifier)
      .withColumn("condition_coding", codingClassify(col("code")("coding")).cast("array<struct<category:string,code:string>>"))
      .withColumn("source_text", col("code")("text"))
      .withColumn("participant_fhir_id", extractReferenceId(col("subject")("reference")))
      .withColumn("source_text_tumor_location", col("bodySite")("text"))
      .withColumn("uberon_id_tumor_location", flatten(transform(col("bodySite")("coding"), c => c("display"))))
      .withColumn("affected_status", col("verificationStatus")("text").cast(BooleanType))
      .withColumn("affected_status_text", col("verificationStatus")("coding")("display")(0))
      .withColumn("down_syndrome_diagnosis", col("verificationStatus")("text"))
      .withColumn("age_at_event", struct(
        col("_recordedDate")("recordedDate")("offset")("value") as "value",
        col("_recordedDate")("recordedDate")("offset")("unit") as "units"))
      .withColumn("diagnosis_mondo", filter(col("condition_coding"), x => x("category") === "MONDO")(0)("code"))
      //**Duplication** needed to rename this fields without breaking other services such as the portal.
      .withColumn("mondo_code", extractThenReformatConditionCoding("MONDO"))
      .withColumn("diagnosis_ncit", filter(col("condition_coding"), x => x("category") === "NCIT")(0)("code"))
      //**Duplication** needed to rename this fields without breaking other services such as the portal.
      .withColumn("ncit_code", extractThenReformatConditionCoding("NCIT"))
      .withColumn("diagnosis_icd", filter(col("condition_coding"), x => x("category") === "ICD")(0)("code"))
      //**Duplication** needed to rename this fields without breaking other services such as the portal.
      .withColumn("icd_code", extractThenReformatConditionCoding("ICD"))
    ),
    Drop("identifier", "code", "subject", "verificationStatus", "_recordedDate", "bodySite", "category")
  )

  val histologyObservationMappings: List[Transformation] = List(
    Custom(_
      .select("study_id", "release_id", "specimen", "subject", "focus")
      .withColumn("condition_id", explode(col("focus.reference")))
      .withColumn("condition_id", extractReferenceId(col("condition_id")))
      .withColumn("specimen_id", extractReferenceId(col("specimen.reference")))
      .withColumn("patient_id", extractReferenceId(col("subject.reference")))),
    Drop("specimen", "subject", "focus")
  )

  val conditionPhenotypeMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "study_id", "release_id", "identifier", "code", "subject", "verificationStatus", "_recordedDate")
      .withColumn("phenotype_id", officialIdentifier)
      .withColumn("condition_coding", codingClassify(col("code")("coding")).cast("array<struct<category:string,code:string>>"))
      .withColumn("source_text", col("code")("text"))
      .withColumn("participant_fhir_id", extractReferenceId(col("subject")("reference")))
      .withColumn("observed", col("verificationStatus")("coding")(0)("code"))
      .withColumn("age_at_event", struct(
        col("_recordedDate")("recordedDate")("offset")("value") as "value",
        col("_recordedDate")("recordedDate")("offset")("unit") as "units",
      ))
    ),
    Drop("identifier", "code", "subject", "verificationStatus", "_recordedDate")
  )

  val organizationMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "release_id", "study_id", "identifier", "name")
      .withColumn("organization_id", officialIdentifier)
      .withColumn("institution", col("name"))
    ),
    Drop("identifier", "name")
  )

  val researchstudyMappings: List[Transformation] = List(
    Custom(_
      .select("fhir_id", "keyword", "release_id", "study_id", "title", "identifier", "principalInvestigator", "status", "relatedArtifact", "category")
      .withColumn("attribution", extractFirstForSystem(col("identifier"), Seq(SYS_NCBI_URL))("value"))
      .withColumn("external_id", extractStudyExternalId(extractFirstForSystem(col("identifier"), Seq(SYS_NCBI_URL))("value")))
      .withColumnRenamed("title", "name")
      .withColumn("version", extractStudyVersion(extractFirstForSystem(col("identifier"), Seq(SYS_NCBI_URL))("value")))
      .withColumn(
        "investigator_id",
        regexp_extract(col("principalInvestigator")("reference"), patternPractitionerRoleResearchStudy, 1)
      )
      .withColumn("study_code_fallback", col("keyword")(1)("coding")(0)("code"))
      .withColumn("study_code_from_system", extractFirstMatchingSystem(flatten(col("keyword.coding")), Seq(SYS_SHORT_CODE_KF))("display"))
      .withColumn("study_code", coalesce(col("study_code_from_system"), col("study_code_fallback")))
      .withColumn("program", extractFirstMatchingSystem(flatten(col("keyword.coding")), Seq(SYS_PROGRAMS_KF, SYS_PROGRAMS_INCLUDE))("display"))
      .withColumn("website", extractDocUrl(col("relatedArtifact"))("url"))
      .withColumn("domain", col("category")(0)("text"))
      .withColumn("biobank_contact", extractFirstMatchingSystem(extractVirtualBiorepositoryContact(col("contact"))("telecom"), Seq("email"))("value"))
      .withColumn("biobank_request_link", extractFirstMatchingSystem(extractVirtualBiorepositoryContact(col("contact"))("telecom"), Seq("url"))("value"))
      .withColumn("note", col("note")(0)("text"))
    ),
    Drop(
      "title",
      "identifier",
      "principalInvestigator",
      "keyword",
      "relatedArtifact",
      "category",
      "study_code_fallback",
      "study_code_from_system"
    )
  )

  val documentreferenceMappings: List[Transformation] = List(
    Custom { input =>
      val df = input
        .select("fhir_id", "study_id", "release_id", "category", "securityLabel", "content", "type", "identifier", "subject", "context", "docStatus", "relatesTo", "full_url")
        .withColumn("access_urls", col("content")("attachment")("url")(0))
        .withColumn("acl", extractAclFromList(col("securityLabel")("text"), col("study_id")))
        .withColumn("controlled_access", firstSystemEquals(flatten(col("securityLabel.coding")), SYS_DATA_ACCESS_TYPES)("display"))
        .withColumn("data_type", firstSystemEquals(col("type")("coding"), SYS_DATA_TYPES)("display"))
        .withColumn("data_category", firstSystemEquals(flatten(col("category")("coding")), SYS_DATA_CATEGORIES)("display"))
        .withColumn("experiment_strategy", firstSystemEquals(flatten(col("category")("coding")), SYS_EXP_STRATEGY)("display"))
        .withColumn("external_id", col("content")(0)("attachment")("url"))
        .withColumn("file_format", firstNonNull(col("content")("format")("display")))
        .withColumn("file_name", sanitizeFilename(firstNonNull(col("content")("attachment")("title"))))
        .withColumn("file_id", officialIdentifier)
        .withColumn("hashes", extractHashes(col("content")(0)("attachment")("hashes")))
        .withColumn("is_harmonized", retrieveIsHarmonized(col("content")(0)("attachment")("url")))
        .withColumn("latest_did", split(col("content")("attachment")("url")(0), "\\/\\/")(2))
        .withColumn("repository", retrieveRepository(col("content")("attachment")("url")(0)))
        .withColumn("size", retrieveSize(col("content")(0)("attachment")("fileSize")))
        .withColumn("urls", col("content")(0)("attachment")("url"))
        .withColumn("s3_url", col("content")(1)("attachment")("url"))
        .withColumn("participant_fhir_id", extractReferenceId(col("subject")("reference")))
        .withColumn("specimen_fhir_ids", extractReferencesId(col("context")("related")("reference")))
        .withColumnRenamed("docStatus", "status")
        .withColumn("relate_to", extractReferencesId(col("relatesTo.target.reference"))(0))
        .withColumn("latest_did", extractLatestDid(col("content")(0)("attachment")("url")))
        .withColumn("fhir_document_reference", {
          val fhirBaseUrl = split(col("full_url"), "/DocumentReference")(0)
          concat(lit(fhirBaseUrl), lit("/DocumentReference?identifier="), col("file_id"))
        })

      val indexes = df.as("index").where(col("file_format").isin("crai", "tbi", "bai"))
      val files = df.as("file").where(not(col("file_format").isin("crai", "tbi", "bai")))

      files
        .join(indexes, col("index.relate_to") === col("file.fhir_id"), "left_outer")
        .select(
          col("file.*"),
          when(
            col("index.relate_to").isNull, lit(null)
          )
            .otherwise(
              struct(col("index.fhir_id") as "fhir_id", col("index.file_name") as "file_name",
                col("index.file_id") as "file_id", col("index.hashes") as "hashes",
                col("index.urls") as "urls", col("index.file_format") as "file_format",
                col("index.size") as "size"
              )
            ) as "index"
        )

    }
    ,
    Drop("securityLabel", "content", "type", "identifier", "subject", "context", "relates_to", "relate_to", "category")
  )

  val groupMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .where(exists(col("identifier"), identifier => identifier("system") === "https://kf-api-dataservice.kidsfirstdrc.org/families/") || exists(col("code.coding"), code => code("code") === "FAMMEMB"))
      .withColumn("external_id", filter(col("identifier"), c => c("system").isNull)(0)("value"))
      .withColumn("family_id", officialIdentifier)
      .withColumn("exploded_member", explode(col("member")))
      .withColumn("exploded_member_entity", extractReferenceId(col("exploded_member")("entity")("reference")))
      .withColumn("exploded_member_inactive", col("exploded_member")("inactive"))
      .withColumn("family_members", struct("exploded_member_entity", "exploded_member_inactive"))
      .withColumn("family_type_from_system", firstSystemEquals(col("code")("coding"), SYS_FAMILY_TYPES)("display"))
      .groupBy("fhir_id", "study_id", "family_id", "external_id", "type", "release_id", "family_type_from_system")
      .agg(
        collect_list("family_members") as "family_members",
        collect_list("exploded_member_entity") as "family_members_id"

      )
    ),
    Drop()
  )


  val probandObservationMappings: List[Transformation] = List(
    Custom(input =>
      input.select("subject", "valueCodeableConcept", "release_id", "study_id")
        .withColumn("participant_fhir_id", extractReferenceId(col("subject")("reference")))
        .withColumn("is_proband", firstSystemEquals(col("valueCodeableConcept")("coding"), SYS_YES_NO)("code") === "Y")
    ),
    Drop("subject", "valueCodeableConcept")
  )

  def extractionMappingsFor(): Map[String, List[Transformation]] = Map(
    "patient" -> patientMappings,
    "vital_status" -> observationVitalStatusMappings,
    "family_relationship" -> observationFamilyRelationshipMappings,
    "phenotype" -> conditionPhenotypeMappings,
    "disease" -> diseaseMappings,
    "research_subject" -> researchSubjectMappings,
    "research_study" -> researchstudyMappings,
    "group" -> groupMappings,
    "document_reference" -> documentreferenceMappings,
    "organization" -> organizationMappings,
    "proband_observation" -> probandObservationMappings,
    "histology_observation" -> histologyObservationMappings,
    "specimen" -> SpecimensTransformations(),
  )
}
