package bio.ferlab.fhir.etl.transformations

import bio.ferlab.datalake.spark3.transformation.{Custom, Drop, Transformation}
import bio.ferlab.fhir.etl.Utils.{codingClassify, extractAclFromList, firstNonNull}
import org.apache.spark.sql.functions.{col, collect_list, explode, filter, regexp_extract, struct}

object Transformations {

  val URL_US_CORE_RACE = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"

  val URL_US_CORE_ETHNICITY = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"
  val URL_FILE_SIZE = "https://nih-ncpi.github.io/ncpi-fhir-ig/StructureDefinition/file-size"
  val patternParticipantStudy = "[A-Z][a-z]+-(SD_[0-9A-Za-z]+)-([A-Z]{2}_[0-9A-Za-z]+)"
  val participantSpecimen = "[A-Z][a-z]+/([0-9A-Za-z]+)"
  val conditionTypeR = "^https:\\/\\/[A-Za-z0-9-_.\\/]+\\/([A-Za-z0-9]+)"

  val patientMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .withColumn("race", filter(col("extension"), c => c("url") === URL_US_CORE_RACE)(0)("extension")(0)("valueString"))
      .withColumn("ethnicity", filter(col("extension"), c => c("url") === URL_US_CORE_ETHNICITY)(0)("extension")(0)("valueString"))
      .withColumn("external_id", col("identifier")(0)("value"))
      .withColumn("study_id", regexp_extract(col("identifier")(2)("value"), patternParticipantStudy, 1))
      .withColumn("participant_id", regexp_extract(col("identifier")(2)("value"), patternParticipantStudy, 2))
    ),
    Drop("extension", "id", "identifier", "meta")
  )

  val specimenMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .withColumn("participant_fhir_id",   regexp_extract(col("subject")("reference"), participantSpecimen, 1))
      .withColumn("specimen_id", col("identifier")(0)("value"))
      .withColumn("composition", col("type")("text"))
    ),
    //    Drop("id", "subject", "identifier")
    Drop()
  )

  val observationMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .withColumn("participant_fhir_id", regexp_extract( col("subject")("reference"), participantSpecimen, 1))
      .withColumn("vital_status", col("valueCodeableConcept")("text"))
      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
      .withColumn("participant_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
    ),
    Drop()
  )

  val conditionMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
      .withColumn("diagnosis_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
      .withColumn("condition_coding", codingClassify(col("code")("coding")))
      .withColumn("source_text", col("code")("text"))
      .withColumn("participant_fhir_id", regexp_extract( col("subject")("reference"), participantSpecimen, 1))
      //could be phenotype OR disease per condition
      .withColumn("condition_profile", regexp_extract(col("meta")("profile")(0), conditionTypeR, 1))
      .withColumn("observed", col("verificationStatus")("text"))
      .withColumn("tumor_location", col("bodySite"))
    ),
    Drop()
  )

  val researchsubjectMappings: List[Transformation] = List(
    Custom(_
      .select("*")
    ),
    Drop()
  )

  val researchstudyMappings: List[Transformation] = List(
    Custom(_
      .select("*")
    ),
    Drop()
  )

  val documentreferenceMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
      .withColumn("genomic_file_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
      .withColumn("acl", extractAclFromList(col("securityLabel")("text")))
      .withColumn("access_urls", col("content")("attachment")("url")(0))
      .withColumn("external_id", col("content")("attachment")("url")(1))
      .withColumn("data_type", col("type")("text"))
      .withColumn("file_format", firstNonNull(col("content")("format")("display")))
      .withColumn("file_name", firstNonNull(col("content")("attachment")("title")))
      .withColumn("size", col("content")("attachment")("extension")(1)("valueDecimal"))
      .withColumn("participant_fhir_id", regexp_extract( col("subject")("reference"), participantSpecimen, 1))
    ),
    Drop("content")
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
    Drop()
  )

  val extractionMappings: Map[String, List[Transformation]] = Map(
    "patient" -> patientMappings,
    "specimen" -> specimenMappings,
    "observation" -> observationMappings,
    "condition" -> conditionMappings,
    "researchsubject" -> researchsubjectMappings,
    "researchstudy" -> researchstudyMappings,
    "group" -> groupMappings,
    "documentreference" -> documentreferenceMappings,
  )

}
