package bio.ferlab.etl.testutils

import bio.ferlab.datalake.commons.config.{ConfigurationLoader, RuntimeETLContext, SimpleConfiguration}
import bio.ferlab.datalake.testutils.{TestETLContext, WithSparkSession}

trait WithTestSimpleConfiguration extends WithSparkSession {
  lazy implicit val conf: SimpleConfiguration = ConfigurationLoader.loadFromResources[SimpleConfiguration]("config/dev-include.conf")

  val defaultRuntime: RuntimeETLContext = TestETLContext()
}
