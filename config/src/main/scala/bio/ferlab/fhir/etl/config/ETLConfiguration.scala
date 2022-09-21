package bio.ferlab.fhir.etl.config

import bio.ferlab.datalake.commons.config.{ConfigurationWrapper, DatalakeConf}

case class ETLConfiguration(datalake: DatalakeConf, excludeSpecimenCollection: Boolean) extends ConfigurationWrapper(datalake) {

}
