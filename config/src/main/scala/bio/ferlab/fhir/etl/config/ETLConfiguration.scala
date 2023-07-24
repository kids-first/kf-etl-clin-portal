package bio.ferlab.fhir.etl.config

import bio.ferlab.datalake.commons.config.{ConfigurationWrapper, DatalakeConf}
import pureconfig.ConfigReader

case class ETLConfiguration(isFlatSpecimenModel: Boolean, datalake: DatalakeConf, dataservice_url: String) extends ConfigurationWrapper(datalake)

object ETLConfiguration {
  import pureconfig.generic.auto._
  import pureconfig.generic.semiauto._

  implicit val configReader: ConfigReader[ETLConfiguration] = deriveReader[ETLConfiguration]

}

