import bio.ferlab.datalake.commons.config.{ConfigurationLoader, SimpleConfiguration}
import pureconfig.generic.auto._
import pureconfig.module.enum._

trait WithTestConfig {
  lazy val conf: SimpleConfiguration = ConfigurationLoader.loadFromResources[SimpleConfiguration]("config/dev-include.conf")
}
