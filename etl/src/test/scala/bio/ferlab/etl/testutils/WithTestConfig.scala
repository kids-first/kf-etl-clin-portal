package bio.ferlab.etl.testutils

import bio.ferlab.datalake.commons.config.{ConfigurationLoader, SimpleConfiguration}
import bio.ferlab.fhir.etl.config.ETLConfiguration
import pureconfig.generic.auto._

trait WithTestConfig {
  lazy val conf: ETLConfiguration = ConfigurationLoader.loadFromResources[ETLConfiguration]("config/dev-include.conf")
}
