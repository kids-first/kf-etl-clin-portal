package bio.ferlab.etl

import bio.ferlab.etl.EsTemplateHelper.updateCentricMappingsProperties
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EsTemplateHelperSpec extends AnyFlatSpec with Matchers {
  val kStudy = "study"
  val kParticipant = "participant"
  val kFile = "file"
  val kBiospecimen = "biospecimen"


  behavior of "updateCentricMappingsProperties"

  it should "manage field ADDITION, DELETION and UPDATE" in {
    val studyTemplatePropsWithNewField = ujson.read(
      """{
        |  "study_id": {
        |    "type": "keyword"
        |  },
        |  "update": {
        |    "type": "nested"
        |  },
        |  "addition": {
        |    "type": "keyword"
        |  }
        |}""".stripMargin
    )
    val fileTemplatePropsBefore = ujson.read(
      """{
        |  "file_id": {
        |    "type": "keyword"
        |  },
        |  "participants": {
        |    "properties": {
        |      "biospecimens": {
        |        "properties": {
        |          "fhir_id": {
        |            "type": "keyword"
        |          }
        |        },
        |        "type": "nested"
        |      },
        |      "participant_id": {
        |        "type": "keyword"
        |      },
        |      "study": {
        |        "properties": {
        |          "study_id": {
        |            "type": "keyword"
        |          },
        |          "delete": {
        |            "type": "keyword"
        |          },
        |          "update": {
        |            "type": "keyword"
        |          }
        |        }
        |      }
        |    },
        |    "type": "nested"
        |  },
        |  "study": {
        |    "properties": {
        |      "study_id": {
        |        "type": "keyword"
        |      },
        |      "delete": {
        |        "type": "keyword"
        |      },
        |      "update": {
        |        "type": "keyword"
        |      }
        |    }
        |  }
        |}""".stripMargin)
    val participantTemplatePropsBefore = ujson.read(
      """{
        |  "files": {
        |    "properties": {
        |      "biospecimens": {
        |        "properties": {
        |          "fhir_id": {
        |            "type": "keyword"
        |          }
        |        },
        |        "type": "nested"
        |      },
        |      "file_id": {
        |        "type": "keyword"
        |      }
        |    },
        |    "type": "nested"
        |  },
        |  "participant_id": {
        |    "normalizer": "custom_normalizer",
        |    "type": "keyword"
        |  },
        |  "study": {
        |    "properties": {
        |      "study_id": {
        |        "type": "keyword"
        |      },
        |      "delete": {
        |        "type": "keyword"
        |      },
        |      "update": {
        |        "type": "keyword"
        |      }
        |    }
        |  }
        |}""".stripMargin
    )
    val biospecimenTemplatePropsBefore = ujson.read(
      """{
        |  "fhir_id": {
        |    "type": "keyword"
        |  },
        |  "files": {
        |    "properties": {
        |      "file_id": {
        |        "type": "keyword"
        |      }
        |    },
        |    "type": "nested"
        |  },
        |  "participant": {
        |    "properties": {
        |      "participant_id": {
        |        "type": "keyword"
        |      },
        |      "study": {
        |        "properties": {
        |          "study_id": {
        |            "type": "keyword"
        |          },
        |          "delete": {
        |            "type": "keyword"
        |          },
        |          "update": {
        |            "type": "keyword"
        |          }
        |        }
        |      }
        |    }
        |  },
        |  "study": {
        |    "properties": {
        |      "study_id": {
        |        "type": "keyword"
        |      },
        |      "delete": {
        |        "type": "keyword"
        |      },
        |      "update": {
        |        "type": "keyword"
        |      }
        |    }
        |  }
        |}""".stripMargin
    )

    val mEntityToTemplateMappingsProps = Map(
      kStudy -> studyTemplatePropsWithNewField,
      kParticipant -> participantTemplatePropsBefore,
      kFile -> fileTemplatePropsBefore,
      kBiospecimen -> biospecimenTemplatePropsBefore
    )

    val m = updateCentricMappingsProperties(mEntityToTemplateMappingsProps)

    val studyCentric = m(kStudy)
    val fileCentric = m(kFile)
    val participantCentric = m(kParticipant)
    val biospecimenCentric = m(kBiospecimen)

    val containCorrectStudyKeys = contain theSameElementsAs Seq("update", "study_id", "addition")
    val includeEsMappingsPropThatNeededToBeUpdated = include("nested")
    val notContainTheKeyThatNeedsToBeDeleted = not contain Seq("delete")

    studyCentric.obj.keys should containCorrectStudyKeys
    studyCentric.obj.keys should notContainTheKeyThatNeedsToBeDeleted
    studyCentric("update")("type").str should includeEsMappingsPropThatNeededToBeUpdated

    fileCentric("study")("properties").obj.keys should containCorrectStudyKeys
    fileCentric("study")("properties").obj.keys should notContainTheKeyThatNeedsToBeDeleted
    fileCentric("study")("properties")("update")("type").str should includeEsMappingsPropThatNeededToBeUpdated
    fileCentric("participants")("properties")("study")("properties").obj.keys should containCorrectStudyKeys
    fileCentric("participants")("properties")("study")("properties").obj.keys should notContainTheKeyThatNeedsToBeDeleted
    fileCentric("participants")("properties")("study")("properties")("update")("type").str should includeEsMappingsPropThatNeededToBeUpdated

    biospecimenCentric("study")("properties").obj.keys should containCorrectStudyKeys
    biospecimenCentric("study")("properties").obj.keys should notContainTheKeyThatNeedsToBeDeleted
    biospecimenCentric("study")("properties")("update")("type").str should includeEsMappingsPropThatNeededToBeUpdated
    biospecimenCentric("participant")("properties")("study")("properties").obj.keys should containCorrectStudyKeys
    biospecimenCentric("participant")("properties")("study")("properties").obj.keys should notContainTheKeyThatNeedsToBeDeleted
    biospecimenCentric("participant")("properties")("study")("properties")("update")("type").str should includeEsMappingsPropThatNeededToBeUpdated

    participantCentric("study")("properties").obj.keys should containCorrectStudyKeys
    participantCentric("study")("properties").obj.keys should notContainTheKeyThatNeedsToBeDeleted
    participantCentric("study")("properties")("update")("type").str should includeEsMappingsPropThatNeededToBeUpdated
  }
}
