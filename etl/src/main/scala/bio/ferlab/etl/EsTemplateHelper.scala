package bio.ferlab.etl

import ujson._
import scala.io.Source

/**
 * CLINICAL templates
 * ------------------
 * Add, update or delete a field from an entity in its => respective template <=
 * then run this script to propagate it through all the templates where the entity exists.
 *
 * For edge cases proceed manually - it is only a helper.
 * */
object EsTemplateHelper extends App {
  val kStudy = "study"
  val kParticipant = "participant"
  val kFile = "file"
  val kBiospecimen = "biospecimen"

  private def extractTemplateProperties(t: ujson.Value): ujson.Value = ujson.copy(
    t("template")("mappings")("properties"))

  private def extractTemplate(rName: String): ujson.Value = ujson
    .copy(
      ujson.read(
        Source
          .fromInputStream(getClass.getResourceAsStream(rName))
          .mkString
      )
    )

  private def updateTemplateMappingsProps(rName: String, mProps: ujson.Value): ujson.Value = {
    val t = extractTemplate(rName)
    t("template")("mappings")("properties") = mProps
    t
  }

  private def getMappingsProps(rPath: String): Value = extractTemplateProperties(extractTemplate(rPath))

  private def sortByKeyShallowly(j: ujson.Value): ujson.Value.Value = {
    //TODO Make a recursive function that traverses the whole object once at the very end.
    ujson.read(ujson.copy(j).obj.toSeq.sortBy(_._1))
  }

  def updateCentricMappingsProperties(mEntityToTemplateMappingsProps: Map[String, Value]) = {
    // not using keys from "global" variables to avoid delayed issues with tests.
    val studyBase = mEntityToTemplateMappingsProps("study")
    val participantBase = {
      val props = mEntityToTemplateMappingsProps("participant")

      props.obj.remove("files")
      props.obj.remove("study")

      props("diagnosis")("properties") = sortByKeyShallowly(props("diagnosis")("properties"))
      props("mondo")("properties") = sortByKeyShallowly(props("mondo")("properties"))
      props("non_observed_phenotype")("properties") = sortByKeyShallowly(props("non_observed_phenotype")("properties"))
      props("observed_phenotype")("properties") = sortByKeyShallowly(props("observed_phenotype")("properties"))
      props("outcomes")("properties") = sortByKeyShallowly(props("outcomes")("properties"))
      props("phenotype")("properties") = sortByKeyShallowly(props("phenotype")("properties"))

      sortByKeyShallowly(props)
    }
    val fileBase = {
      val props = mEntityToTemplateMappingsProps("file")

      props.obj.remove("participants")
      props.obj.remove("study")

      props("hashes")("properties") = sortByKeyShallowly(props("hashes")("properties"))
      props("index")("properties") = sortByKeyShallowly(props("index")("properties"))
      props("sequencing_experiment")("properties") = sortByKeyShallowly(props("sequencing_experiment")("properties"))

      sortByKeyShallowly(props)
    }
    val biospecimenBase = {
      val props = mEntityToTemplateMappingsProps("biospecimen")

      props.obj.remove("files")
      props.obj.remove("participant")
      props.obj.remove("study")

      sortByKeyShallowly(props)
    }

    def addStudy(j: ujson.Value): Value.Value = {
      val jc = ujson.copy(j)
      jc("study") = ujson.Obj("properties" -> studyBase)
      sortByKeyShallowly(jc)
    }

    val participantBaseWithStudy = addStudy(participantBase)
    val fileBaseWithStudy = addStudy(fileBase)

    val studyCentric = ujson.copy(studyBase)

    val fileCentric = {
      val f = ujson.copy(fileBaseWithStudy)
      f("participants") = ujson.Obj("type" -> "nested", "properties" -> {
        val ps = ujson.copy(participantBaseWithStudy)
        ps("biospecimens") = ujson.Obj("type" -> "nested", "properties" -> ujson.copy(biospecimenBase))
        sortByKeyShallowly(ps)
      })
      sortByKeyShallowly(f)
    }
    val biospecimenCentric = {
      val biospecimenBaseWithStudy = addStudy(biospecimenBase)
      val b = ujson.copy(biospecimenBaseWithStudy)
      b("participant") = ujson.Obj("properties" -> participantBaseWithStudy)
      b("files") = ujson.Obj("type" -> "nested", "properties" -> fileBase)
      sortByKeyShallowly(b)
    }
    val participantCentric = {
      val p = ujson.copy(participantBaseWithStudy)
      p("files") = ujson.Obj("type" -> "nested", "properties" -> {
        val fs = ujson.copy(fileBase)
        fs("biospecimens") = ujson.Obj("type" -> "nested", "properties" -> ujson.copy(biospecimenBase))
        sortByKeyShallowly(fs)
      })
      sortByKeyShallowly(p)
    }

    Map(
      "study" -> studyCentric,
      "participant" -> participantCentric,
      "file" -> fileCentric,
      "biospecimen" -> biospecimenCentric
    )
  }

  private val mEntityToTemplatePath = Map(
    kStudy -> "/templates/template_study_centric.json",
    kParticipant -> "/templates/template_participant_centric.json",
    kFile -> "/templates/template_file_centric.json",
    kBiospecimen -> "/templates/template_biospecimen_centric.json"
  )

  private val mEntityToTemplateMappingsProps = Map(
    kStudy -> getMappingsProps(mEntityToTemplatePath(kStudy)),
    kParticipant -> getMappingsProps(mEntityToTemplatePath(kParticipant)),
    kFile -> getMappingsProps(mEntityToTemplatePath(kFile)),
    kBiospecimen -> getMappingsProps(mEntityToTemplatePath(kBiospecimen))
  )

  private val centric = updateCentricMappingsProperties(mEntityToTemplateMappingsProps)

  print(
    """
      |Choose the template you want to print :
      |  1) study centric
      |  2) participant centric
      |  3) file centric
      |  4) biospecimen centric
      |""".stripMargin)
  private val templateChoice = scala.io.StdIn.readLine("> ")
  private val chosenTemplate = templateChoice.trim match {
    case "1" => updateTemplateMappingsProps(mEntityToTemplatePath(kStudy), centric(kStudy))
    case "2" => updateTemplateMappingsProps(mEntityToTemplatePath(kParticipant), centric(kParticipant))
    case "3" => updateTemplateMappingsProps(mEntityToTemplatePath(kFile), centric(kFile))
    case "4" => updateTemplateMappingsProps(mEntityToTemplatePath(kBiospecimen), centric(kBiospecimen))
    case _ => ujson.read("""{}""")
  }

  print("Do you want the template to be printed as one-liner? y/n :\n")
  private val oneLinerChoice = scala.io.StdIn.readLine("> ")
  private val isOneliner = oneLinerChoice.toLowerCase.trim match {
    case "y" => true
    case _ => false
  }

  if (isOneliner) {
    println("\n" + ujson.write(chosenTemplate))
  } else {
    println("+----------------------------------------------------------------------------+")
    println(ujson.write(chosenTemplate, indent = 4))
    println("+----------------------------------------------------------------------------+")
  }
}
