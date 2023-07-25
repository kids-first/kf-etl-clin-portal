package bio.ferlab.etl.prepared.clinical

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{arg, main}

object RunPrepareClinical {
  @main(name = "study_centric", doc = "Prepare Index Study Centric")
  def studyCentric(rc: RuntimeETLContext, @arg(name = "study-id", short = 's', doc = "Study Id") studies: List[String]): Unit = StudyCentric(rc, studies).run()

  @main(name = "simple_participant", doc = "Prepare Simple Participant")
  def simpleParticipant(rc: RuntimeETLContext, @arg(name = "study-id", short = 's', doc = "Study Id") studies: List[String]): Unit = SimpleParticipant(rc, studies).run()

  @main(name = "participant_centric", doc = "Prepare Index Participant Centric")
  def participantCentric(rc: RuntimeETLContext, @arg(name = "study-id", short = 's', doc = "Study Id") studies: List[String]): Unit = ParticipantCentric(rc, studies).run()

  @main(name = "biospecimen_centric", doc = "Prepare Index Biospecimen Centric")
  def biospecimenCentric(rc: RuntimeETLContext, @arg(name = "study-id", short = 's', doc = "Study Id") studies: List[String]): Unit = BiospecimenCentric(rc, studies).run()

  @main(name = "file_centric", doc = "Prepare Index File Centric")
  def fileCentric(rc: RuntimeETLContext, @arg(name = "study-id", short = 's', doc = "Study Id") studies: List[String]): Unit = FileCentric(rc, studies).run()


  @main(name = "all", doc = "Run all prepare index")
  def all(rc: RuntimeETLContext, @arg(name = "study-id", short = 's', doc = "Study Id") studies: List[String]): Unit = {
    simpleParticipant(rc, studies)
    studyCentric(rc, studies)
    participantCentric(rc, studies)
    biospecimenCentric(rc, studies)
    fileCentric(rc, studies)
  }


}
