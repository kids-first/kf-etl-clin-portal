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

  def updateCentricMappingsProperties(mEntityToTemplateMappingsProps: Map[String, Value]) = {
    // not using keys from "global" variables to avoid delayed issues with tests.
    val studyBase = mEntityToTemplateMappingsProps("study")
    val participantBase = {
      val props = mEntityToTemplateMappingsProps("participant")
      props.obj.remove("files")
      props.obj.remove("study")
      props
    }
    val fileBase = {
      val props = mEntityToTemplateMappingsProps("file")
      props.obj.remove("participants")
      props.obj.remove("study")
      props
    }
    val biospecimenBase = {
      val props = mEntityToTemplateMappingsProps("biospecimen")
      props.obj.remove("files")
      props.obj.remove("participant")
      props.obj.remove("study")
      props
    }

    def addStudy(j: ujson.Value): Value.Value = {
      val jc = ujson.copy(j)
      jc("study") = ujson.Obj("properties" -> studyBase)
      jc
    }

    val participantBaseWithStudy = addStudy(participantBase)
    val fileBaseWithStudy = addStudy(fileBase)

    val studyCentric = ujson.copy(studyBase)
    val fileCentric = {
      val f = ujson.copy(fileBaseWithStudy)
      f("participants") = ujson.Obj("type" -> "nested", "properties" -> {
        val ps = ujson.copy(participantBaseWithStudy)
        ps("biospecimens") = ujson.Obj("type" -> "nested", "properties" -> ujson.copy(biospecimenBase))
        ps
      })
      f
    }
    val biospecimenCentric = {
      val biospecimenBaseWithStudy = addStudy(biospecimenBase)
      val b = ujson.copy(biospecimenBaseWithStudy)
      b("participant") = ujson.Obj("properties" -> participantBaseWithStudy)
      b("files") = ujson.Obj("type" -> "nested", "properties" -> fileBase)
      b
    }
    val participantCentric = {
      val p = ujson.copy(participantBaseWithStudy)
      p("files") = ujson.Obj("type" -> "nested", "properties" -> {
        val fs = ujson.copy(fileBase)
        fs("biospecimens") = ujson.Obj("type" -> "nested", "properties" -> ujson.copy(biospecimenBase))
        fs
      })
      p
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

  //WIP, TODO: override templates
  val studyTemplate = updateTemplateMappingsProps(mEntityToTemplatePath(kStudy), centric(kStudy))
  print("===== Study Template \n")
  //println(ujson.write(studyTemplate, indent = 4))
  val fileTemplate = updateTemplateMappingsProps(mEntityToTemplatePath(kFile), centric(kFile))
  print("===== File Template \n")
  //println(ujson.write(fileTemplate, indent = 4))
  val participantTemplate = updateTemplateMappingsProps(mEntityToTemplatePath(kParticipant), centric(kParticipant))
  print("===== Participant Template \n")
  //println(ujson.write(participantTemplate, indent = 4))
  val biospecimenTemplate = updateTemplateMappingsProps(mEntityToTemplatePath(kBiospecimen), centric(kBiospecimen))
  print("===== Biospecimen Template \n")
  //println(ujson.write(biospecimenTemplate, indent = 4))
}
