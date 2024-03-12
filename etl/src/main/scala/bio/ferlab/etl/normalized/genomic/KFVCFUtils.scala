package bio.ferlab.etl.normalized.genomic

import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.SparkUtils.filename
import bio.ferlab.fhir.etl.config.StudyConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.GZIPInputStream

object KFVCFUtils {
  /**
   * Load the vcf files from the given dataframe. The dataframe must contains a column named "s3_url" which contains the S3 path to the vcf file.
   *
   * @param files               The dataframe containing the S3 path to the vcf files.
   * @param studyId             the study id used to filter the files.
   * @param referenceGenomePath a path to reference genome file used to align variants.
   * @param spark
   * @return
   */
  def loadVCFs(files: DataFrame, studyConfiguration: StudyConfiguration, studyId: String, referenceGenomePath: Option[String] = None)(implicit spark: SparkSession): DataFrame = {
    val vcfFiles: Seq[VCFFiles] = getVCFFiles(files, studyConfiguration, studyId)

    vcfFiles.map(_.load(referenceGenomePath))
      .reduce { (df1, df2) => df1.unionByName(df2, true) }
      .withColumn("file_name", filename)
  }

  def extractBucketNames(list: Seq[String]): Seq[(String, String)] = {
    val bucketRegex = "^(s3a://[^/]+)/.*$".r

    list.map { str =>
      val bucket = str.replaceAll(bucketRegex.toString, "$1")
      (str, bucket)
    }
  }

  private def initFileSystems(buckets: Set[String])(implicit spark: SparkSession): Map[String, FileSystem] = {
    buckets.map(b => b -> FileSystem.get(new java.net.URI(b), spark.sparkContext.hadoopConfiguration)).toMap
  }

  /**
   * Get the versions of the vcf files. The version is inferred from the first line of each vcf file.
   *
   * @param files              a dataframe containing the s3 urls of the vcf files.
   * @param studyId            id of the study used to filter the files.
   * @param studyConfiguration configuration of the study.
   * @param spark
   * @return a list of VCFFiles containing the version and the list of files.
   */
  private def getVCFFiles(files: DataFrame, studyConfiguration: StudyConfiguration, studyId: String)(implicit spark: SparkSession): Seq[VCFFiles] = {
    import spark.implicits._


    val filesUrl = files
      .where(col("study_id") === studyId and col("s3_url").rlike(studyConfiguration.snvVCFPattern))
      .select("s3_url")
      .distinct()
      .as[String].collect()
      .collect { case s if s != null => s.replace("s3://", "s3a://") }

    val filesUrlWithBucket = extractBucketNames(filesUrl)
    val buckets = filesUrlWithBucket.map { case (_, bucket) => bucket }.toSet
    val fileSystemByBuckets = initFileSystems(buckets)

    val filesWithVersion: Seq[(String, VCFVersion)] = filesUrlWithBucket.par.map { case (file, bucket) =>
      calculateFileVersion(bucket, file, fileSystemByBuckets)
    }.toList

    val filesVersion: Seq[VCFFiles] = filesWithVersion.groupBy { case (_, version) => version }
      .map { case (version, files) => VCFFiles(version, files.map { case (fileUrl, _) => fileUrl }.toList) }
      .toList

    filesVersion

  }

  private def calculateFileVersion(bucket: String, file: String, fileSystemByBuckets: Map[String, FileSystem]): (String, VCFVersion) = {
    val path = new Path(file)
    val inputStream = fileSystemByBuckets(bucket).open(path)
    val gzipInputStream = new GZIPInputStream(inputStream)
    val reader = new BufferedReader(new InputStreamReader(gzipInputStream))
    try {

      val lines = Iterator.continually(reader.readLine()).take(3500)
      val version = calculateVersionFromHeaders(lines)
      (file, version)
    } finally {
      reader.close()
    }
  }

  def calculateVersionFromHeaders(it: Iterator[String]): VCFVersion = {
    val lines = it.take(3500).toSeq
    val containsInfoCSQ = lines.exists(line => line.contains("##INFO=<ID=CSQ"))
    val containsInfoDS = lines.exists(line => line.contains("##INFO=<ID=DS"))
    val containsInfoPG = lines.exists(line => line.contains("##INFO=<ID=PG"))
    val version = (containsInfoCSQ, containsInfoDS, containsInfoPG) match {
      case (false, false, _) => V1
      case (false, true, true) => V2
      case (false, true, false) => V2_WITHOUT_PG
      case (true, _, _) => V3
    }
    version
  }


  private case class VCFFiles(version: VCFVersion, files: Seq[String]) {
    def load(referenceGenomePath: Option[String])(implicit spark: SparkSession): DataFrame = {
      val df = vcf(files.toList, referenceGenomePath)
      version.loadVersion(df)
    }
  }

  trait VCFVersion {
    def loadVersion(df: DataFrame): DataFrame
  }

  case object V1 extends VCFVersion {
    override def loadVersion(df: DataFrame): DataFrame = {
      df
        .withColumn("INFO_DS", lit(null).cast("boolean"))
        .withColumn("INFO_HaplotypeScore", lit(null).cast("double"))
        .withColumn("genotype", explode(col("genotypes")))
        .drop("genotypes")
        .withColumn("INFO_ReadPosRankSum", col("INFO_ReadPosRankSum")(0))
        .withColumn("INFO_ClippingRankSum", col("INFO_ClippingRankSum")(0))
        .withColumn("INFO_RAW_MQ", col("INFO_RAW_MQ")(0))
        .withColumn("INFO_BaseQRankSum", col("INFO_BaseQRankSum")(0))
        .withColumn("INFO_MQRankSum", col("INFO_MQRankSum")(0))
        .withColumn("INFO_ExcessHet", col("INFO_ExcessHet")(0))
        .withColumn(
          "genotype",
          struct(
            col("genotype.sampleId"),
            col("genotype.conditionalQuality"),
            col("genotype.filters"),
            col("genotype.SB"),
            col("genotype.alleleDepths"),
            col("genotype.PP"),
            col("genotype.PID")(0) as "PID",
            col("genotype.phased"),
            col("genotype.calls"),
            col("genotype.MIN_DP")(0) as "MIN_DP",
            col("genotype.JL"),
            col("genotype.PGT")(0) as "PGT",
            col("genotype.phredLikelihoods"),
            col("genotype.depth"),
            col("genotype.RGQ"),
            col("genotype.JP")
          )
        )
    }
  }

  case object V2 extends VCFVersion {
    override def loadVersion(df: DataFrame): DataFrame = {
      df
        .withColumn("genotype", explode(col("genotypes")))
        .drop("genotypes")
    }
  }

  case object V2_WITHOUT_PG extends VCFVersion {
    override def loadVersion(df: DataFrame): DataFrame = {
      df
        .withColumn("genotype", explode(col("genotypes")))
        .withColumn("INFO_PG", lit(null).cast(ArrayType(IntegerType)))
        .drop("genotypes")
    }
  }

  case object V3 extends VCFVersion {
    override def loadVersion(df: DataFrame): DataFrame = {
      df
        .drop("INFO_ANN")
        .withColumn("genotype", explode(col("genotypes")))
        .withColumn("INFO_ANN", transform(col("INFO_CSQ"), csq =>
          struct(
            csq("Allele") as "Allele",
            csq("Consequence") as "Consequence",
            csq("IMPACT") as "IMPACT",
            csq("SYMBOL") as "SYMBOL",
            csq("Gene") as "Gene",
            csq("Feature_type") as "Feature_type",
            when(csq("SOURCE") === "Ensembl", csq("Feature")).otherwise(lit(null)) as "Feature",
            csq("BIOTYPE") as "BIOTYPE",
            csq("EXON") as "EXON",
            csq("INTRON") as "INTRON",
            csq("HGVSc") as "HGVSc",
            csq("HGVSp") as "HGVSp",
            csq("cDNA_position") as "cDNA_position",
            csq("CDS_position") as "CDS_position",
            csq("Protein_position") as "Protein_position",
            csq("Amino_acids") as "Amino_acids",
            csq("Codons") as "Codons",
            csq("Existing_variation") as "Existing_variation",
            csq("DISTANCE") as "DISTANCE",
            csq("STRAND") as "STRAND",
            csq("FLAGS") as "FLAGS",
            csq("VARIANT_CLASS") as "VARIANT_CLASS",
            csq("SYMBOL_SOURCE") as "SYMBOL_SOURCE",
            csq("HGNC_ID") as "HGNC_ID",
            csq("CANONICAL") as "CANONICAL",
            csq("SIFT") as "SIFT",
            csq("HGVS_OFFSET") as "HGVS_OFFSET",
            csq("HGVSg") as "HGVSg",
            when(csq("SOURCE") === "RefSeq", csq("Feature")).otherwise(csq("RefSeq")) as "RefSeq",
            csq("PUBMED") as "PUBMED",
            csq("PICK") as "PICK"
          )
        ))
        .drop("genotypes", "INFO_CSQ")
    }
  }
}
