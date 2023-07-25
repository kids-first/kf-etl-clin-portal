package bio.ferlab.etl.prepared.clinical

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import bio.ferlab.etl.mainutils.Studies
import mainargs.main

object RunPrepareClinical {
  @main(name = "study_centric", doc = "Prepare Index Study Centric")
  def studyCentric(rc: RuntimeETLContext, studies: Studies): Unit = StudyCentric(rc, studies.ids).run()

  @main(name = "simple_participant", doc = "Prepare Simple Participant")
  def simpleParticipant(rc: RuntimeETLContext, studies: Studies): Unit = SimpleParticipant(rc, studies.ids).run()

  @main(name = "participant_centric", doc = "Prepare Index Participant Centric")
  def participantCentric(rc: RuntimeETLContext, studies: Studies): Unit = ParticipantCentric(rc, studies.ids).run()

  @main(name = "biospecimen_centric", doc = "Prepare Index Biospecimen Centric")
  def biospecimenCentric(rc: RuntimeETLContext, studies: Studies): Unit = BiospecimenCentric(rc, studies.ids).run()

  @main(name = "file_centric", doc = "Prepare Index File Centric")
  def fileCentric(rc: RuntimeETLContext, studies: Studies): Unit = FileCentric(rc, studies.ids).run()


  @main(name = "all", doc = "Run all prepare index")
  def all(rc: RuntimeETLContext, studies: Studies): Unit = {
    simpleParticipant(rc, studies)
    studyCentric(rc, studies)
    participantCentric(rc, studies)
    biospecimenCentric(rc, studies)
    fileCentric(rc, studies)
  }


}
