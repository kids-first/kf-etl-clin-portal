package bio.ferlab.fhir.etl.config

import bio.ferlab.datalake.commons.config.{ConfigurationWrapper, DatalakeConf}

case class ETLConfiguration(excludeSpecimenCollection: Boolean, datalake: DatalakeConf) extends ConfigurationWrapper(datalake)
