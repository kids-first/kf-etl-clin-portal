package bio.ferlab.etl

import ujson._

import scala.io.Source
import scala.util.chaining.scalaUtilChainingOps
/**
 * CLINICAL templates
 * ------------------
 * Add, update or delete a field from an entity in its => respective template <=
 * then run this script to propagate it through all the templates where the entity exists.
 *
 * For edge cases proceed manually - it is only a helper.
 * */
object EsTemplateHelper extends App {
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
  implicit class Helper(s: String) {
    def getMappingsProps: Value = s.pipe(extractTemplate).pipe(extractTemplateProperties)
  }

  val mTemplate = Map(
    "study" -> "/templates/template_study_centric.json",
    "participant" -> "/templates/template_participant_centric.json",
    "file" -> "/templates/template_file_centric.json",
    "biospecimen" -> "/templates/template_biospecimen_centric.json"
  )

  val studyBase = mTemplate("study").getMappingsProps
  val participantBase = {
    val props = mTemplate("participant").getMappingsProps
    props.obj.remove("files")
    props.obj.remove("study")
    props
  }
  val fileBase = {
    val props = mTemplate("file").getMappingsProps
    props.obj.remove("participants")
    props.obj.remove("study")
    props
  }
  val biospecimenBase = {
    val props = mTemplate("biospecimen").getMappingsProps
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
  val biospecimenBaseWithStudy = addStudy(biospecimenBase)

  val studyCentric = ujson.copy(studyBase)
  val fileCentric = {
    val f = ujson.copy(fileBaseWithStudy)
    f("participants") = ujson.Obj("type" -> "nested", "properties" -> {
      val ps = ujson.copy(participantBaseWithStudy)
      ps("biospecimens") = ujson.Obj("type" -> "nested", "properties" -> ujson.copy(biospecimenBaseWithStudy))
      ps
    })
    f
  }
  val biospecimenCentric = {
    val b = ujson.copy(biospecimenBaseWithStudy)
    b("participant") = ujson.Obj("properties" -> participantBaseWithStudy)
    b("files") = ujson.Obj("type" -> "nested", "properties" -> fileBaseWithStudy)
    b
  }
  val participantCentric = {
    val p = ujson.copy(participantBaseWithStudy)
    p("files") = ujson.Obj("type" -> "nested", "properties" -> {
      val fs = ujson.copy(fileBaseWithStudy)
      fs("biospecimens") = ujson.Obj("type" -> "nested", "properties" -> ujson.copy(biospecimenBaseWithStudy))
      fs
    })
    p
  }

  //WIP, TODO: override templates
  val studyTemplate = updateTemplateMappingsProps(mTemplate("study"), studyCentric)
  print("===== Study Template \n")
  println(ujson.write(studyTemplate, indent = 4))
  val fileTemplate = updateTemplateMappingsProps(mTemplate("file"), fileCentric)
  print("===== File Template \n")
  println(ujson.write(fileTemplate, indent = 4))
  val participantTemplate = updateTemplateMappingsProps(mTemplate("participant"), participantCentric)
  print("===== Participant Template \n")
  println(ujson.write(participantTemplate, indent = 4))
  val biospecimenTemplate = updateTemplateMappingsProps(mTemplate("biospecimen"), biospecimenCentric)
  print("===== Biospecimen Template \n")
  println(ujson.write(biospecimenTemplate, indent = 4))
}
