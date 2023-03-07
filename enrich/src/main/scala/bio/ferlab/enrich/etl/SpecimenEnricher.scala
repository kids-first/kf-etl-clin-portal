package bio.ferlab.enrich.etl

import org.apache.spark.sql.functions.{col, collect_list, explode, regexp_extract, struct}
import org.apache.spark.sql.{Column, DataFrame, functions}

object SpecimenEnricher {
  private def extractId(column: Column): Column =
    functions.split(column, "/")(1) //FIXME duplicate across modules

  private def generateFhirIdColumValueFromIdColum(): Column =
    regexp_extract(
      col("id"),
      "^https?:\\/\\/.*/(\\p{Alnum}+)/_history[\\?|/].*$",
      1
    ) //FIXME duplicate across modules

  def specimensToHistoPathologies(
                                   histDf: DataFrame,
                                   mondoDiseasesDf: DataFrame,
                                   ncitDiseasesDf: DataFrame
                                 ): DataFrame = {

    val df = histDf
      .select(
        extractId(col("specimen.reference")) as "specimen_id",
        extractId(col("subject.reference")) as "patient_id",
        explode(col("focus.reference")) as "condition_id"
      )
      .withColumn("condition_id", extractId(col("condition_id")))

    val shapeDiseasesTmpDf = (diseasesDf: DataFrame) => diseasesDf
      .withColumn("fhir_id", generateFhirIdColumValueFromIdColum())
      .select(
        col("fhir_id"),
        struct(col("code")) as "disease"
      )
    val shapedMondoDf = shapeDiseasesTmpDf(mondoDiseasesDf)
    val shapedNcitDf = shapeDiseasesTmpDf(ncitDiseasesDf)

    val shapeEnhancedSpecimenTmpDf = (diseasesDf: DataFrame, diseasesColName: String) =>
      df
        .join(diseasesDf, df("condition_id") === diseasesDf("fhir_id"), "left")
        .drop(diseasesDf("fhir_id"))
        .groupBy(col("specimen_id"))
        .agg(collect_list(col("disease")) as diseasesColName)
    val specimenWithMondoDf = shapeEnhancedSpecimenTmpDf(shapedMondoDf, "diseases_mondo")
    val specimenWithNcitDf = shapeEnhancedSpecimenTmpDf(shapedNcitDf, "diseases_ncit")

    specimenWithMondoDf.join(specimenWithNcitDf, Seq("specimen_id"), "full")
  }
}