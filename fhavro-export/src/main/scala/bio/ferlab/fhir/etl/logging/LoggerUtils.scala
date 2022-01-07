package bio.ferlab.fhir.etl.logging

import java.util.concurrent.atomic.AtomicInteger

object LoggerUtils {

  def logProgress(verb: String, progress: Int): Unit = {
    print(s"[main] INFO ${getClass.getName} - %s resource(s) ${verb}ed.\r".format(progress))
  }

  def logProgressAtomic(verb: String, progress: AtomicInteger, total: Int): Unit = {
    print(s"[main] INFO ${getClass.getName} - %s%% ${verb}ed.\r".format((progress.incrementAndGet().floatValue() / total) * 100))
  }
}
