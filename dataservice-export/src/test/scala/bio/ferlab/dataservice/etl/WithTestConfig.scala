package bio.ferlab.dataservice.etl

import bio.ferlab.datalake.commons.config.{ConfigurationLoader, SimpleConfiguration}
import pureconfig.generic.auto._

trait WithTestConfig {
  lazy val conf: SimpleConfiguration = ConfigurationLoader.loadFromResources[SimpleConfiguration]("config/dev-include.conf")
}
