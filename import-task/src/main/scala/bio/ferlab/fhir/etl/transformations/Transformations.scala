package bio.ferlab.fhir.etl.transformations

import bio.ferlab.datalake.spark3.transformation.{Custom, Drop, Transformation}
import bio.ferlab.fhir.etl.Utils.{extractAclFromList, extractHashes, firstNonNull, ncitIdAnatomicalSite, retrieveIsHarmonized, uberonIdAnatomicalSite}
import org.apache.spark.sql.functions._

object Transformations {

  val URL_US_CORE_RACE = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"

  val URL_US_CORE_ETHNICITY = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"
  val URL_FILE_SIZE = "https://nih-ncpi.github.io/ncpi-fhir-ig/StructureDefinition/file-size"
  val URL_HASHES = "https://nih-ncpi.github.io/ncpi-fhir-ig/StructureDefinition/hashes"
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
//    Drop("extension", "id", "identifier", "meta")
    Drop()
  )

  val specimenMappings: List[Transformation] = List(
    Custom(_
      .select("collection")
      .withColumn("participant_fhir_id",   regexp_extract(col("subject")("reference"), participantSpecimen, 1))
      .withColumn("specimen_id", col("identifier")(0)("value"))
      .withColumn("composition", col("type")("text"))
      .withColumn("method_of_sample_procurement", col("collection")("method")("coding"))
      .withColumn("source_text_anatomical_site", col("collection")("bodySite")("text"))
      .withColumn("ncit_id_anatomical_site", ncitIdAnatomicalSite(col("collection")("bodySite")("coding")))
      .withColumn("uberon_id_anatomical_site", uberonIdAnatomicalSite(col("collection")("bodySite")("coding")))
      .withColumn("volume_ul", col("collection")("quantity")("value"))
      .withColumn("volume_ul_unit", col("collection")("quantity")("unit"))
//      .withColumn("external_aliquot_id", col("identifier")) //FIXME
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
      .select("bodySite")
            .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
            .withColumn("diagnosis_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
            .withColumn("condition_coding", codingClassify(col("code")("coding")))
            .withColumn("source_text", col("code")("text"))
            .withColumn("participant_fhir_id", regexp_extract( col("subject")("reference"), participantSpecimen, 1))
            //could be phenotype OR disease per condition
            .withColumn("observed", col("verificationStatus")("text"))
            .withColumn("source_text_tumor_location", col("bodySite")("text"))
            .withColumn("uberon_id_tumor_location", col("bodySite")("coding")) //TODO
            .withColumn("condition_profile", regexp_extract(col("meta")("profile")(0), conditionTypeR, 1))
      //      .withColumn("snomed_id_phenotype", col("code")) //TODO
      //      .withColumn("external_id", col("identifier")) //TODO
      //      .withColumn("diagnosis_category", col("code")) //TODO
    ),
    Drop("bodySite")
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
      .withColumn("acl", extractAclFromList(col("securityLabel")("text")))
      .withColumn("access_urls", col("content")("attachment")("url")(0))
      // TODO availability
      // TODO controlled_access
      // TODO created_at
      .withColumn("data_type", col("type")("text"))
      .withColumn("external_id", col("content")(1)("attachment")("url"))
      .withColumn("file_format", firstNonNull(col("content")("format")("display")))
      .withColumn("file_name", firstNonNull(col("content")("attachment")("title")))
      .withColumn("genomic_file_fhir_id", col("fhir_id"))
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
      // TODO repository
      .withColumn("size", filter(col("content")(1)("attachment")("extension"), c => c("url") === URL_FILE_SIZE)("valueDecimal")(0))
      .withColumn("urls", col("content")(1)("attachment")("url"))
      // TODO visible
      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
      // TODO release_id
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
