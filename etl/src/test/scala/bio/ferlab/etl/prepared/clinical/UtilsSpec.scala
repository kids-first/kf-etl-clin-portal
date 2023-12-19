package bio.ferlab.etl.prepared.clinical

import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.etl.prepared.clinical.Utils._
import bio.ferlab.etl.testmodels.enriched._
import bio.ferlab.etl.testmodels.normalized._
import bio.ferlab.etl.testmodels.prepared._
import bio.ferlab.etl.testutils.WithTestSimpleConfiguration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode_outer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UtilsSpec extends AnyFlatSpec with Matchers with WithTestSimpleConfiguration {
  import spark.implicits._

  case class ConditionCoding(code: String, category: String)

  val SCHEMA_CONDITION_CODING = "array<struct<category:string,code:string>>"
  val allHpoTerms: DataFrame = read(getClass.getResource("/hpo_terms.json").toString, "Json", Map(), None, None)
  val allMondoTerms: DataFrame = read(getClass.getResource("/mondo_terms.json.gz").toString, "Json", Map(), None, None)

  "addStudy" should "add studies to participant" in {
    val inputStudies = Seq(NORMALIZED_RESEARCHSTUDY()).toDF()
    val inputParticipants = Seq(NORMALIZED_PATIENT()).toDF()

    val output = inputParticipants.addStudy(inputStudies)

    output.collect().sameElements(Seq(PREPARED_PARTICIPANT()))
  }

  "addSequencingExperiment" should "add sequencing experiment to DF" in {
    val inputDocumentReference = Seq(
      NORMALIZED_DOCUMENTREFERENCE(file_id = "file1"),
      NORMALIZED_DOCUMENTREFERENCE(file_id = "file2"),
      NORMALIZED_DOCUMENTREFERENCE(file_id = "file3", `experiment_strategy` = null),
    ).toDF()
    val inputSeqExp = Seq(
      NORMALIZED_SEQUENCING_EXPERIMENT(kf_id = "seq_exp1"),
      NORMALIZED_SEQUENCING_EXPERIMENT(kf_id = "seq_exp2", experiment_strategy = "WES"),
      NORMALIZED_SEQUENCING_EXPERIMENT(kf_id = "seq_exp3"),
    ).toDF()
    val inputSeqExpGenFile = Seq(
      NORMALIZED_SEQUENCING_EXPERIMENT_GENOMIC_FILE(sequencing_experiment = "seq_exp1", genomic_file = "file1"),
      NORMALIZED_SEQUENCING_EXPERIMENT_GENOMIC_FILE(sequencing_experiment = "seq_exp2", genomic_file = "file1"),
      NORMALIZED_SEQUENCING_EXPERIMENT_GENOMIC_FILE(sequencing_experiment = "seq_exp3", genomic_file = "file2")
    ).toDF()

    val output = inputDocumentReference.addSequencingExperiment(inputSeqExp, inputSeqExpGenFile)
      .as[PREPARED_DOCUMENTREFERENCE]
      .collect()
      .toSeq

    output.length shouldBe 3

    output.find(_.file_id === "file1") should not be empty
    val seqExpResultFile1 = output.find(_.file_id === "file1").get.sequencing_experiment
    seqExpResultFile1.length shouldBe 2
    seqExpResultFile1 should contain theSameElementsAs Seq(
      PREPARED_SEQUENCING_EXPERIMENT(sequencing_experiment_id = "seq_exp1"),
      PREPARED_SEQUENCING_EXPERIMENT(sequencing_experiment_id = "seq_exp2", experiment_strategy = "WES")
    )

    output.find(_.file_id === "file2") should not be empty
    val seqExpResultFile2 = output.find(_.file_id === "file2").get.sequencing_experiment
    seqExpResultFile2.length shouldBe 1
    seqExpResultFile2 should contain theSameElementsAs Seq(
      PREPARED_SEQUENCING_EXPERIMENT(sequencing_experiment_id = "seq_exp3")
    )

    output.find(_.file_id === "file3") should not be empty
    output.find(_.file_id === "file3").get.sequencing_experiment shouldBe null
  }

  it should "fallback to sequencing experiment defined in document reference fhir resource" in {
    val inputDocumentReference = Seq(
      NORMALIZED_DOCUMENTREFERENCE(file_id = "file1", `experiment_strategy` = "WXS")
    ).toDF()
    val inputSeqExp = spark.emptyDataset[NORMALIZED_SEQUENCING_EXPERIMENT].toDF()
    val inputSeqExpGenomicFile = spark.emptyDataset[NORMALIZED_SEQUENCING_EXPERIMENT_GENOMIC_FILE].toDF()

    val output = inputDocumentReference.addSequencingExperiment(inputSeqExp, inputSeqExpGenomicFile)
      .as[PREPARED_DOCUMENTREFERENCE]
      .collect()
      .toSeq

    output.length shouldBe 1

    output.find(_.file_id === "file1") should not be empty
    val seqExpResultFile1 = output.find(_.file_id === "file1").get.sequencing_experiment
    assert(seqExpResultFile1 != null, "Sequencing experiment should not be null")
    seqExpResultFile1.length shouldBe 1
    seqExpResultFile1 should contain theSameElementsAs Seq(
      PREPARED_SEQUENCING_EXPERIMENT(sequencing_experiment_id = null, experiment_strategy = "WXS")
    )


  }

  "addOutcomes" should "add outcomes to participant" in {
    val inputPatients = Seq(
      NORMALIZED_PATIENT(`fhir_id` = "P1"),
      NORMALIZED_PATIENT(`fhir_id` = "P2")
    ).toDF()

    val inputObservationVitalStatus = Seq(
      NORMALIZED_VITAL_STATUS(`fhir_id` = "O1", `participant_fhir_id` = "P1"),
      NORMALIZED_VITAL_STATUS(`fhir_id` = "O3", `participant_fhir_id` = "P_NOT_THERE")
    ).toDF()

    val output = inputPatients.addOutcomes(inputObservationVitalStatus)

    val patientWithOutcome = output.select("fhir_id", "outcomes").as[(String, Seq[PREPARED_OUTCOME])].collect()

    val patient1 = patientWithOutcome.filter(_._1 == "P1").head
    val patient2 = patientWithOutcome.filter(_._1 == "P2").head
    patientWithOutcome.exists(_._1 == "P_NOT_THERE") shouldBe false

    patient1._2.map(_.`fhir_id`) shouldEqual Seq("O1")
    patient2._2.isEmpty shouldBe true
  }

  "addDownSyndromeDiagnosis" should "add down syndrome diagnosis to dataframe" in {
    val inputPatients = Seq(
      NORMALIZED_PATIENT(`fhir_id` = "P1"),
      NORMALIZED_PATIENT(`fhir_id` = "P2"),
      NORMALIZED_PATIENT(`fhir_id` = "P3")
    ).toDF()

    val mondoTerms = Seq(
      NORMALIZED_ONTOLOGY_TERM("MONDO:0000000", "Another Term"),
      NORMALIZED_ONTOLOGY_TERM("MONDO:0008608", "Down Syndrome"),
      NORMALIZED_ONTOLOGY_TERM("MONDO:0008609", "Down Syndrome level 2", `ancestors` = Seq(TERM("MONDO:0008608", "Down Syndrome")))
    ).toDF()

    val inputDiseases = Seq(
      NORMALIZED_DISEASE(`fhir_id` = "O1", `participant_fhir_id` = "P1", `mondo_code` = Some("MONDO:0008608")),
      NORMALIZED_DISEASE(`fhir_id` = "O2", `participant_fhir_id` = "P1", `mondo_code` = Some("MONDO:0008609")),
      NORMALIZED_DISEASE(`fhir_id` = "O3", `participant_fhir_id` = "P2", `mondo_code` = Some("MONDO:0008609")),
      NORMALIZED_DISEASE(`fhir_id` = "O4", `participant_fhir_id` = "P2", `mondo_code` = Some("MONDO:0000000")),
      NORMALIZED_DISEASE(`fhir_id` = "O5", `participant_fhir_id` = "P3", `mondo_code` = Some("MONDO:0000000"))
    ).toDF()

    val output = inputPatients.addDownSyndromeDiagnosis(inputDiseases, mondoTerms)

    val patientWithDS = output.select("fhir_id", "down_syndrome_status", "down_syndrome_diagnosis").as[(String, String, Seq[String])].collect()

    patientWithDS.find(_._1 == "P1") shouldBe Some(
      ("P1", "T21", Seq("Down Syndrome (MONDO:0008608)", "Down Syndrome level 2 (MONDO:0008609)"))
    )
    patientWithDS.find(_._1 == "P2") shouldBe Some(
      ("P2", "T21", Seq("Down Syndrome level 2 (MONDO:0008609)"))
    )
    patientWithDS.find(_._1 == "P3") shouldBe Some(
      ("P3", "D21", null)
    )
  }

  "addFamily" should "add enriched family to patients" in {
    //start of inputs ===== //
    val rawInputHasNoKnownFamilyTypePatient = Seq(
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f01", `participant_id` = "p01"),
    )

    val rawInputProbandOnlyPatient = Seq(
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f11", `participant_id` = "p11", `is_proband` = true),
    )

    val rawInputDuoPatients = Seq(
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f21", `participant_id` = "p21", `is_proband` = true),
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f22", `participant_id` = "p22", `gender` = "female"),
    )
    val rawInputDuoFamilyEnriched = Seq(
      ENRICHED_FAMILY(
        family_fhir_id = "ff21",
        participant_fhir_id = "f21",
        relations = Seq(
          RELATION(`participant_id` = "p21", `role` = "proband"),
          RELATION(`participant_id` = "p22", `role` = "mother")
        )
      ),
      ENRICHED_FAMILY(
        family_fhir_id = "ff21",
        participant_fhir_id = "f22",
        relations = Seq(
          RELATION(`participant_id` = "p21", `role` = "proband"),
          RELATION(`participant_id` = "p22", `role` = "mother")
        )
      )
    )
    val rawInputDuoGroup = Seq(
      NORMALIZED_GROUP(
        `fhir_id` = "ff21",
        `family_id` = "ff21",
        `family_members` = Seq(("f21", false), ("f22", false)),
        `family_members_id` = Seq("f21", "f22"),
      ),
    )

    val rawInputDuoPlusPatients = Seq(
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f21+", `participant_id` = "p21+", `is_proband` = true),
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f22+", `participant_id` = "p22+"),
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f23+", `participant_id` = "p23+"),
    )
    val rawInputDuoPlusFamilyEnriched = Seq(
      ENRICHED_FAMILY(
        family_fhir_id = "ff21+",
        participant_fhir_id = "f21+",
        relations = Seq(
          RELATION(`participant_id` = "p21+", `role` = "proband"),
          RELATION(`participant_id` = "p22+", `role` = "father"),
          RELATION(`participant_id` = "p23+", `role` = "sibling"),
        )
      ),
      ENRICHED_FAMILY(
        family_fhir_id = "ff21+",
        participant_fhir_id = "f22+",
        relations = Seq(
          RELATION(`participant_id` = "p21+", `role` = "proband"),
          RELATION(`participant_id` = "p22+", `role` = "father"),
          RELATION(`participant_id` = "p23+", `role` = "sibling"),
        )
      ),
      ENRICHED_FAMILY(
        family_fhir_id = "ff21+",
        participant_fhir_id = "f23+",
        relations = Seq(
          RELATION(`participant_id` = "p21+", `role` = "proband"),
          RELATION(`participant_id` = "p22+", `role` = "father"),
          RELATION(`participant_id` = "p23+", `role` = "sibling"),
        )
      )
    )
    val rawInputDuoPlusGroup = Seq(
      NORMALIZED_GROUP(
        `fhir_id` = "ff21+",
        `family_id` = "ff21+",
        `family_members` = Seq(("f21+", false), ("f22+", false), ("f23+", false)),
        `family_members_id` = Seq("f21+", "f22+", "f23+"),
      ),
    )

    val rawInputTrioPatients = Seq(
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f31", `participant_id` = "p31", `is_proband` = true),
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f32", `participant_id` = "p32", `gender` = "female"),
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f33", `participant_id` = "p33"),
    )
    val rawInputTrioFamilyEnriched = Seq(
      ENRICHED_FAMILY(
        family_fhir_id = "ff31",
        participant_fhir_id = "f31",
        relations = Seq(
          RELATION(`participant_id` = "p31", `role` = "proband"),
          RELATION(`participant_id` = "p32", `role` = "mother"),
          RELATION(`participant_id` = "p33", `role` = "father")
        )
      ),
      ENRICHED_FAMILY(
        family_fhir_id = "ff31",
        participant_fhir_id = "f32",
        relations = Seq(
          RELATION(`participant_id` = "p31", `role` = "proband"),
          RELATION(`participant_id` = "p32", `role` = "mother"),
          RELATION(`participant_id` = "p33", `role` = "father")
        )
      ),
      ENRICHED_FAMILY(
        family_fhir_id = "ff31",
        participant_fhir_id = "f33",
        relations = Seq(
          RELATION(`participant_id` = "p31", `role` = "proband"),
          RELATION(`participant_id` = "p32", `role` = "mother"),
          RELATION(`participant_id` = "p33", `role` = "father")
        )
      )
    )

    val rawInputTrioGroup = Seq(
      NORMALIZED_GROUP(
        `fhir_id` = "ff31",
        `family_id` = "ff31",
        `family_members` = Seq(("f31", false), ("f32", false), ("f33", false)),
        `family_members_id` = Seq("f31", "f32", "f33"),
      ),
    )

    val rawInputTrioPlusPatients = Seq(
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f31+", `participant_id` = "p31+", `is_proband` = true),
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f32+", `participant_id` = "p32+", `gender` = "female"),
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f33+", `participant_id` = "p33+"),
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f34+", `participant_id` = "p34+"),
    )
    val rawInputTrioPlusFamilyPlusEnriched = Seq(
      ENRICHED_FAMILY(
        family_fhir_id = "ff31+",
        participant_fhir_id = "f31+",
        relations = Seq(
          RELATION(`participant_id` = "p31+", `role` = "proband"),
          RELATION(`participant_id` = "p32+", `role` = "mother"),
          RELATION(`participant_id` = "p33+", `role` = "father"),
          RELATION(`participant_id` = "p34+", `role` = "sibling")
        )
      ),
      ENRICHED_FAMILY(
        family_fhir_id = "ff31+",
        participant_fhir_id = "f32+",
        relations = Seq(
          RELATION(`participant_id` = "p31+", `role` = "proband"),
          RELATION(`participant_id` = "p32+", `role` = "mother"),
          RELATION(`participant_id` = "p33+", `role` = "father"),
          RELATION(`participant_id` = "p34+", `role` = "sibling")
        )
      ),
      ENRICHED_FAMILY(
        family_fhir_id = "ff31+",
        participant_fhir_id = "f33+",
        relations = Seq(
          RELATION(`participant_id` = "p31+", `role` = "proband"),
          RELATION(`participant_id` = "p32+", `role` = "mother"),
          RELATION(`participant_id` = "p33+", `role` = "father"),
          RELATION(`participant_id` = "p34+", `role` = "sibling")
        )
      ),
      ENRICHED_FAMILY(
        family_fhir_id = "ff31+",
        participant_fhir_id = "f34+",
        relations = Seq(
          RELATION(`participant_id` = "p31+", `role` = "proband"),
          RELATION(`participant_id` = "p32+", `role` = "mother"),
          RELATION(`participant_id` = "p33+", `role` = "father"),
          RELATION(`participant_id` = "p34+", `role` = "sibling")
        )
      )
    )

    val rawInputTrioPlusGroup = Seq(
      NORMALIZED_GROUP(
        `fhir_id` = "ff31+",
        `family_id` = "ff31+",
        `family_members` = Seq(("f31+", false), ("f32+", false), ("f33+", false), ("f34+", false)),
        `family_members_id` = Seq("f31+", "f32+", "f33+", "f34+"),
      ),
    )

    val rawInputHasFamilyTypeFromSystemPatients = Seq(
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f01s", `participant_id` = "p01s", `is_proband` = true),
      INTERNAL_PATIENT_WITH_PROBAND_INFO(`fhir_id` = "f02s", `participant_id` = "p02s", `gender` = "female"),
    )

    val rawInputHasFamilyTypeFromSystemGroup = Seq(
      NORMALIZED_GROUP(
        `fhir_id` = "ff01s",
        `family_id` = "ff01s",
        `family_members` = Seq(("f01s", false), ("f02s", false)),
        `family_members_id` = Seq("f01s", "f02s"),
        `family_type_from_system` = Some("alpha")
      ),
    )

    val rawInputHasFamilyTypeFromSystemEnriched = Seq(
      ENRICHED_FAMILY(
        family_fhir_id = "ff01s",
        participant_fhir_id = "f01s",
        relations = Seq(
          RELATION(`participant_id` = "p01s", `role` = "proband"),
          RELATION(`participant_id` = "p02s", `role` = "sibling")
        )
      ),
      ENRICHED_FAMILY(
        family_fhir_id = "ff01s",
        participant_fhir_id = "f02s",
        relations = Seq(
          RELATION(`participant_id` = "p01s", `role` = "proband"),
          RELATION(`participant_id` = "p02s", `role` = "sibling")
        )
      )
    )
    //===== end of inputs//
    val inputPatients = (
      rawInputHasNoKnownFamilyTypePatient
        ++ rawInputProbandOnlyPatient
        ++ rawInputDuoPatients
        ++ rawInputDuoPlusPatients
        ++ rawInputTrioPatients
        ++ rawInputTrioPlusPatients
        ++ rawInputHasFamilyTypeFromSystemPatients
      ).toDF()
    val inputEnrichedFamilies = (
      rawInputDuoFamilyEnriched
        ++ rawInputDuoPlusFamilyEnriched
        ++ rawInputTrioFamilyEnriched
        ++ rawInputTrioPlusFamilyPlusEnriched
        ++ rawInputHasFamilyTypeFromSystemEnriched
      ).toDF()
    val inputGroups = (
      rawInputDuoGroup
        ++ rawInputDuoGroup
        ++ rawInputDuoPlusGroup
        ++ rawInputTrioGroup
        ++ rawInputTrioPlusGroup
        ++ rawInputHasFamilyTypeFromSystemGroup
      ).toDF()

    val results = inputPatients
      .addFamily(inputGroups, inputEnrichedFamilies)
      .select(
        col("fhir_id"),
        col("family"),
        col("family_type")
      )
      .as[(String, PREPARED_FAMILY, String)]
      .collect()

    results.length should be > 0

    val extractFamilyTypeForParticipant = (participantId: String) => {
      results.find(x => x._1 == participantId).get._3
    }
    extractFamilyTypeForParticipant(rawInputHasNoKnownFamilyTypePatient.head.`fhir_id`) shouldEqual null
    extractFamilyTypeForParticipant(rawInputProbandOnlyPatient.head.`fhir_id`)shouldEqual "proband-only"

    val extractDistinctFamilyTypesFromAllFamilyMembers = (familyId: String) => {
      results.filter(x => x._2 != null && x._2.family_id == familyId).map(x => x._3).toSet
    }
    extractDistinctFamilyTypesFromAllFamilyMembers(rawInputDuoGroup.head.fhir_id) shouldBe Set("duo")
    extractDistinctFamilyTypesFromAllFamilyMembers(rawInputDuoPlusGroup.head.fhir_id) shouldBe Set("duo+")
    extractDistinctFamilyTypesFromAllFamilyMembers(rawInputTrioGroup.head.fhir_id) shouldBe Set("trio")
    extractDistinctFamilyTypesFromAllFamilyMembers(rawInputTrioPlusGroup.head.fhir_id) shouldBe Set("trio+")
    extractDistinctFamilyTypesFromAllFamilyMembers(rawInputHasFamilyTypeFromSystemGroup.head.fhir_id) shouldBe Set("alpha")
  }

  "addDiagnosisPhenotypes" should "group phenotypes by observed or non-observed" in {

    val inputParticipants = Seq(
      NORMALIZED_PATIENT(participant_id = "A", fhir_id = "A"),
      NORMALIZED_PATIENT(participant_id = "B", fhir_id = "B")
    ).toDF()

    val inputPhenotypes = Seq(
      NORMALIZED_PHENOTYPE(fhir_id = "1p", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0001631")), observed = "confirmed"),
      NORMALIZED_PHENOTYPE(fhir_id = "2p", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING())),
      NORMALIZED_PHENOTYPE(fhir_id = "3p", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING()), observed = "not"),
      NORMALIZED_PHENOTYPE(fhir_id = "4p", participant_fhir_id = "A")
    ).toDF()

    val inputDiseases = Seq.empty[NORMALIZED_DISEASE].toDF()

    val output = inputParticipants.addDiagnosisPhenotypes(inputPhenotypes, inputDiseases)(allHpoTerms, allMondoTerms)
    val participantPhenotypes = output.select("participant_id", "phenotype").as[(String, Seq[PREPARED_PHENOTYPE])].collect()

    val participantA_Ph = participantPhenotypes.filter(_._1 == "A").head
    participantA_Ph._2.map(p => (p.fhir_id, p.`is_observed`)) should contain theSameElementsAs Seq(("1p", true), ("2p", false), ("3p", false))

    val participantB_Ph = participantPhenotypes.find(_._1 == "B")
    participantB_Ph shouldBe Some(("B", null))
  }

  "addDiagnosisPhenotypes" should "take HPO title from hpo file" in {

    val inputParticipants = Seq(
      NORMALIZED_PATIENT(participant_id = "A", fhir_id = "A")
    ).toDF()

    val inputPhenotypes = Seq(
      NORMALIZED_PHENOTYPE(fhir_id = "1p", participant_fhir_id = "A", `source_text` = "source_text", condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0001631")), observed = "confirmed"),
    ).toDF()

    val inputDiseases = Seq.empty[NORMALIZED_DISEASE].toDF()

    val output = inputParticipants.addDiagnosisPhenotypes(inputPhenotypes, inputDiseases)(allHpoTerms, allMondoTerms)

    val result = output.select("participant_id", "phenotype").as[(String, Seq[PREPARED_PHENOTYPE])].collect()

    result.head._2.head.`hpo_phenotype_observed` shouldBe "Atrial septal defect (HP:0001631)"
  }

  it should "map diseases to participants" in {
    val inputParticipants = Seq(
      NORMALIZED_PATIENT(participant_id = "A", fhir_id = "A"),
      NORMALIZED_PATIENT(participant_id = "B", fhir_id = "B")
    ).toDF()

    val inputPhenotypes = Seq.empty[NORMALIZED_PHENOTYPE].toDF()

    val inputDiseases = Seq(
      NORMALIZED_DISEASE(fhir_id = "1d", diagnosis_id = "diag1", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING(`category` = "ICD", `code` = "Q90.9"))),
      NORMALIZED_DISEASE(fhir_id = "2d", diagnosis_id = "diag2", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING(`category` = "NCIT", `code` = "Some NCIT"))),
      NORMALIZED_DISEASE(fhir_id = "3d", diagnosis_id = "diag3", participant_fhir_id = "A")
    ).toDF()

    val output = inputParticipants.addDiagnosisPhenotypes(inputPhenotypes, inputDiseases)(allHpoTerms, allMondoTerms)

    val participantDiseases =
      output
        .select("participant_id", "diagnosis")
        .withColumn("diagnosis_exp", explode_outer(col("diagnosis")))
        .select("participant_id", "diagnosis_exp.diagnosis_id")
        .as[(String, String)].collect()

    val participantA_D = participantDiseases.filter(_._1 == "A")
    val participantB_D = participantDiseases.filter(_._1 == "B").head

    participantA_D.map(_._2) should contain theSameElementsAs Seq("diag1", "diag2")
    participantB_D._2 shouldBe null
  }

  it should "generate observed_phenotypes and non_observed_phenotypes" in {
    val inputParticipants = Seq(
      NORMALIZED_PATIENT(participant_id = "A", fhir_id = "A")
    ).toDF()

    val inputPhenotypes = Seq(
      NORMALIZED_PHENOTYPE(
        fhir_id = "1p",
        participant_fhir_id = "A",
        condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0000234")),
        observed = "confirmed"
      ),
      NORMALIZED_PHENOTYPE(
        fhir_id = "2p",
        participant_fhir_id = "A",
        condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0033127")),
        observed = "not",
        age_at_event = AGE_AT_EVENT(5)
      ),
      NORMALIZED_PHENOTYPE(
        fhir_id = "3p",
        participant_fhir_id = "A",
        condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0002086")),
        age_at_event = AGE_AT_EVENT(10)
      )
    ).toDF()

    val inputDiseases = Seq.empty[NORMALIZED_DISEASE].toDF()

    val output =
      inputParticipants
        .addDiagnosisPhenotypes(inputPhenotypes, inputDiseases)(allHpoTerms, allMondoTerms)
        .select("participant_id", "observed_phenotype", "non_observed_phenotype")
        .as[(String, Seq[PREPARED_ONTOLOGY_TERM], Seq[PREPARED_ONTOLOGY_TERM])].collect()


    val (_, observedPheno, nonObservedPheno) = output.filter(_._1 == "A").head

    observedPheno.count(_.`is_tagged`) shouldBe 1
    assert(observedPheno.forall(_.`age_at_event_days` == Seq(0)))

    nonObservedPheno.count(_.`is_tagged`) shouldBe 2
    nonObservedPheno.flatMap(_.`age_at_event_days`).distinct should contain only(5, 10)
  }

  it should "group diagnosis by age at event days" in {
    val inputParticipants = Seq(
      NORMALIZED_PATIENT(participant_id = "A", fhir_id = "A")
    ).toDF()

    val inputPhenotypes = Seq.empty[NORMALIZED_PHENOTYPE].toDF()

    val inputDiseases = Seq(
      NORMALIZED_DISEASE(
        fhir_id = "1d",
        diagnosis_id = "diag1",
        participant_fhir_id = "A",
        condition_coding = Seq(CONDITION_CODING(`category` = "MONDO", `code` = "MONDO_0002051")),
        mondo_code = Some("MONDO:0002051"),
        age_at_event = AGE_AT_EVENT(5),
      ),
      NORMALIZED_DISEASE(
        fhir_id = "2d",
        diagnosis_id = "diag2",
        participant_fhir_id = "A",
        condition_coding = Seq(CONDITION_CODING(`category` = "MONDO", `code` = "MONDO_0024458")),
        mondo_code = Some("MONDO:0024458"),
        age_at_event = AGE_AT_EVENT(10),
      ),
      NORMALIZED_DISEASE(fhir_id = "3d", diagnosis_id = "diag3", participant_fhir_id = "A")
    ).toDF()

    val output =
      inputParticipants
        .addDiagnosisPhenotypes(inputPhenotypes, inputDiseases)(allHpoTerms, allMondoTerms)
        .select("participant_id", "mondo")
        .as[(String, Seq[PREPARED_ONTOLOGY_TERM])].collect()

    val participantA_Ph = output.filter(_._1 == "A").head

    participantA_Ph._2.filter(t => t.`name` === "disease or disorder (MONDO:0000001)").head.`age_at_event_days` shouldEqual Seq(5, 10)
  }

  "addBiospecimenParticipant" should "add participant - only one" in {
    val inputBiospecimen = Seq(
      PREPARED_BIOSPECIMEN_FOR_FILE(`participant_fhir_id` = "A", `fhir_id` = "1"),
      PREPARED_BIOSPECIMEN_FOR_FILE(`participant_fhir_id` = "B", `fhir_id` = "2"),
      PREPARED_BIOSPECIMEN_FOR_FILE(`participant_fhir_id` = "C", `fhir_id` = "3")
    ).toDF()

    val inputParticipant = Seq(
      PREPARED_SIMPLE_PARTICIPANT(`fhir_id` = "A", participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "A", participant_fhir_id_2 = "A"), `participant_id` = "P_A"),
      PREPARED_SIMPLE_PARTICIPANT(`fhir_id` = "B", participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "B", participant_fhir_id_2 = "B"), `participant_id` = "P_B")
    ).toDF()

    val output = inputBiospecimen.addBiospecimenParticipant(inputParticipant)

    val biospecimenWithParticipant = output.select("fhir_id", "participant").as[(String, PREPARED_SIMPLE_PARTICIPANT)].collect()
    val biospecimen1 = biospecimenWithParticipant.filter(_._1 == "1").head
    val biospecimen2 = biospecimenWithParticipant.filter(_._1 == "2").head

    biospecimen1._2.`participant_id` shouldEqual "P_A"
    biospecimen2._2.`participant_id` shouldEqual "P_B"

    // Ignore biospecimen without participant
    biospecimenWithParticipant.exists(_._1 == "3") shouldEqual false
  }

  "addParticipantFilesWithBiospecimen" should "add files with their biospecimen for a specific participant" in {
    // Input data

    // F1 -> B11, B12, B21
    // F2 -> B11, B13, B31, B32
    // F3 -> B22
    // F4 -> B33
    // F5 -> No biospecimen

    // P1 -> B11, B12, B13
    // P2 -> B21, B22
    // P3 -> B31, B32, B33
    // P4 -> No biospecimen
    // P5 -> No file

    // F6, F7 and B_NOT_THERE1 are related to a missing participant (should be ignored)

    val inputParticipant = Seq(
      PREPARED_SIMPLE_PARTICIPANT(`fhir_id` = "P1"),
      PREPARED_SIMPLE_PARTICIPANT(`fhir_id` = "P2"),
      PREPARED_SIMPLE_PARTICIPANT(`fhir_id` = "P3"),
      PREPARED_SIMPLE_PARTICIPANT(`fhir_id` = "P4"),
      PREPARED_SIMPLE_PARTICIPANT(`fhir_id` = "P5")
    ).toDF()

    val inputBiospecimen = Seq(
      NORMALIZED_BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B11"),
      NORMALIZED_BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B12"),
      NORMALIZED_BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B13"),
      NORMALIZED_BIOSPECIMEN(`participant_fhir_id` = "P2", `fhir_id` = "B21"),
      NORMALIZED_BIOSPECIMEN(`participant_fhir_id` = "P2", `fhir_id` = "B22"),
      NORMALIZED_BIOSPECIMEN(`participant_fhir_id` = "P3", `fhir_id` = "B31"),
      NORMALIZED_BIOSPECIMEN(`participant_fhir_id` = "P3", `fhir_id` = "B32"),
      NORMALIZED_BIOSPECIMEN(`participant_fhir_id` = "P3", `fhir_id` = "B33"),

      NORMALIZED_BIOSPECIMEN(`participant_fhir_id` = "P_NOT_THERE", `fhir_id` = "B_NOT_THERE1")
    ).toDF()

    val inputDocumentReference = Seq(
      PREPARED_DOCUMENTREFERENCE(`participant_fhir_id` = null, `fhir_id` = "F1", `specimen_fhir_ids` = Seq("B11", "B12", "B21")),
      PREPARED_DOCUMENTREFERENCE(`participant_fhir_id` = "P1", `fhir_id` = "F2", `specimen_fhir_ids` = Seq("B11", "B13", "B31", "B32")),
      PREPARED_DOCUMENTREFERENCE(`participant_fhir_id` = "P2", `fhir_id` = "F3", `specimen_fhir_ids` = Seq("B22")),
      PREPARED_DOCUMENTREFERENCE(`participant_fhir_id` = "P3", `fhir_id` = "F4", `specimen_fhir_ids` = Seq("B33")),
      PREPARED_DOCUMENTREFERENCE(`participant_fhir_id` = "P2", `fhir_id` = "F5", `specimen_fhir_ids` = Seq.empty),

      PREPARED_DOCUMENTREFERENCE(`participant_fhir_id` = "P_NOT_THERE", `fhir_id` = "F6", `specimen_fhir_ids` = Seq("B_NOT_THERE1")),
      PREPARED_DOCUMENTREFERENCE(`participant_fhir_id` = "P_NOT_THERE", `fhir_id` = "F7", `specimen_fhir_ids` = Seq.empty),
    ).toDF()

    val enrichedHistology =  Seq(ENRICHED_HISTOLOGY_DISEASE(`specimen_id` = "B11")).toDF()

    val output = inputParticipant.addParticipantFilesWithBiospecimen(inputDocumentReference, inputBiospecimen, enrichedHistology)
    val participantWithFileAndSpecimen = output.select("fhir_id", "files").as[(String, Seq[FILE_WITH_BIOSPECIMEN])].collect()

    // Assertions
    // P1 -> F1 -> B11 & B12
    // P1 -> F2 -> B11 & B13
    // P2 -> F1 -> B21
    // P2 -> F3 -> B22
    // P2 -> F5
    // P3 -> F2 -> B31 & B32
    // P3 -> F4 -> B33
    // P3 -> F5
    // P4 -> F5
    // P5 -> No file
    // P_NOT_THERE should not be there

    val participant1 = participantWithFileAndSpecimen.filter(_._1 == "P1").head
    val participant2 = participantWithFileAndSpecimen.filter(_._1 == "P2").head
    val participant3 = participantWithFileAndSpecimen.filter(_._1 == "P3").head
    val participant4 = participantWithFileAndSpecimen.filter(_._1 == "P4").head
    val participant5 = participantWithFileAndSpecimen.filter(_._1 == "P5").head

    participantWithFileAndSpecimen.exists(_._1 == "P_NOT_THERE") shouldBe false

    participant1._2.map(_.`fhir_id`) == Seq("F1", "F2")
    participant2._2.map(_.`fhir_id`) == Seq("F1", "F3", "F5")
    participant3._2.map(_.`fhir_id`) == Seq("F2", "F4", "F5")
    participant4._2.map(_.`fhir_id`) == Seq("F5")
    participant5._2.isEmpty shouldBe true

    val participantP1FileF1 = participant1._2.filter(_.`fhir_id`.contains("F1")).head
    participantP1FileF1.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B11", "B12")
    val participantP1FileF1WithSpecimenDiagnoses = participantP1FileF1.`biospecimens`.filter(b => b.`diagnoses` != null && b.`diagnoses`.nonEmpty)
    participantP1FileF1WithSpecimenDiagnoses.map(_.`diagnoses`.head.`diagnosis_mondo`) should contain(Some("MONDO:0005072"))
    participantP1FileF1WithSpecimenDiagnoses.map(_.`diagnoses`.head.`diagnosis_ncit`) should contain(Some("NCIT:0005072"))
    participantP1FileF1WithSpecimenDiagnoses.map(_.`diagnoses`.head.`source_text`) should contain(Some("Neuroblastoma"))
    participantP1FileF1WithSpecimenDiagnoses.map(_.`diagnoses`.head.`source_text_tumor_location`) should contain(Seq("Reported Unknown"))

    val participantP1FileF2 = participant1._2.filter(_.`fhir_id`.contains("F2")).head
    participantP1FileF2.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B11", "B13")

    val participantP2FileF1 = participant2._2.filter(_.`fhir_id`.contains("F1")).head
    participantP2FileF1.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B21")

    val participantP2FileF3 = participant2._2.filter(_.`fhir_id`.contains("F3")).head
    participantP2FileF3.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B22")

    val participantP2FileF5 = participant2._2.filter(_.`fhir_id`.contains("F5")).head
    participantP2FileF5.`biospecimens`.isEmpty shouldBe true

    val participantP3FileF2 = participant3._2.filter(_.`fhir_id`.contains("F2")).head
    participantP3FileF2.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B31", "B32")

    val participantP3FileF4 = participant3._2.filter(_.`fhir_id`.contains("F4")).head
    participantP3FileF4.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B33")

  }

  it should "add empty files for biospecimen without files for a specific participant" in {
    val inputParticipant = Seq(
      PREPARED_SIMPLE_PARTICIPANT(`fhir_id` = "P1"),
    ).toDF()

    val inputBiospecimen = Seq(
      NORMALIZED_BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B11"),
      NORMALIZED_BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B12"), //No file associated
    ).toDF()

    val inputDocumentReference = Seq(
      PREPARED_DOCUMENTREFERENCE(`participant_fhir_id` = "P1", `fhir_id` = "F1", `specimen_fhir_ids` = Seq("B11")),
    ).toDF()

    val enrichedHistology =  Seq(ENRICHED_HISTOLOGY_DISEASE(`specimen_id` = "111")).toDF()


    val output = inputParticipant.addParticipantFilesWithBiospecimen(inputDocumentReference, inputBiospecimen, enrichedHistology)

    //B11 and B12 should be attached to P1
    val participant1AndSpecimen = output.select("fhir_id", "files.biospecimens").filter(col("fhir_id") === "P1").as[(String, Seq[Seq[PREPARED_BIOSPECIMEN_FOR_FILE]])].collect()
    participant1AndSpecimen.head._2.flatten.map(_.fhir_id) should contain theSameElementsAs Seq("B11", "B12")

    //P1 should contain one file and one dummy file
    val participantWithFile = output.select("fhir_id", "files.file_name").filter(col("fhir_id") === "P1").as[(String, Seq[String])].collect()
    participantWithFile.head._2 should contain theSameElementsAs Seq("4db9adf4-94f7-4800-a360-49eda89dfb62.g.vcf.gz", "dummy_file")
  }
}

