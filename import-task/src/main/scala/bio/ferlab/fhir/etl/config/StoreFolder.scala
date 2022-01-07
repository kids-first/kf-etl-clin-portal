package bio.ferlab.fhir.etl.config

import bio.ferlab.datalake.commons.config.DatasetConf

trait StoreFolder {

  val alias: String

  private def getClassFields: Set[Any] =
    this.getClass.getDeclaredFields
      .foldLeft(List.empty[Any]) { case (acc, f) =>
        f.setAccessible(true)
        acc :+ f.get(this)
      }
      .toSet

  def sources: Set[DatasetConf] =
    getClassFields.filter(_.isInstanceOf[DatasetConf]).map(_.asInstanceOf[DatasetConf])
}