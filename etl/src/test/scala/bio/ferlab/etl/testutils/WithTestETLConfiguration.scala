package bio.ferlab.etl.testutils

import bio.ferlab.datalake.commons.config.ConfigurationLoader
import bio.ferlab.fhir.etl.config.ETLConfiguration

trait WithTestETLConfiguration {
  lazy val conf: ETLConfiguration = ConfigurationLoader.loadFromResources[ETLConfiguration]("config/dev-include.conf")


}
