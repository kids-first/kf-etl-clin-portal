package bio.ferlab.etl.normalized

import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.SparkUtils.filename
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
   * @param vcfPattern          the pattern used to filter the files.
   * @param referenceGenomePath a path to reference genome file used to align variants.
   * @param spark
   * @return
   */
  def loadVCFs(files: DataFrame, studyId: String, vcfPattern: String, referenceGenomePath: Option[String] = None)(implicit spark: SparkSession): DataFrame = {
    val vcfFiles: Seq[VCFFiles] = getVCFFiles(files, studyId, vcfPattern)

    vcfFiles.map(_.load(referenceGenomePath))
      .reduce { (df1, df2) => df1.unionByName(df2) }
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
   * @param files    a dataframe containing the s3 urls of the vcf files.
   * @param studyId  id of the study used to filter the files.
   * @param endsWith the pattern to filter the files.
   * @param spark
   * @return a list of VCFFiles containing the version and the list of files.
   */
  private def getVCFFiles(files: DataFrame, studyId: String, endsWith: String)(implicit spark: SparkSession): Seq[VCFFiles] = {
    import spark.implicits._
    val filesUrl = files.select("s3_url")
      .where(col("study_id") === studyId and col("s3_url").isNotNull)
      .distinct()
      .as[String].collect()
      .collect { case s if s != null && s.endsWith(endsWith) => s.replace("s3://", "s3a://") }

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

      val lines = Iterator.continually(reader.readLine()).take(200)
      val version = calculateVersionFromHeaders(lines)
      (file, version)
    } finally {
      reader.close()
    }
  }

  def calculateVersionFromHeaders(it: Iterator[String]): VCFVersion = {
    val lines = it.take(200).toSeq
    val containsInfoPG = lines.exists(line => line.contains("##INFO=<ID=PG"))
    val containsInfoDS = lines.exists(line => line.contains("##INFO=<ID=DS"))
    val version = (containsInfoDS, containsInfoPG) match {
      case (false, _) => V1
      case (true, true) => V2
      case (true, false) => V2_WITHOUT_PG
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
      df.drop("annotation", "INFO_ANN")
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
    }
  }

  case object V2_WITHOUT_PG extends VCFVersion {
    override def loadVersion(df: DataFrame): DataFrame = {
      df
        .withColumn("genotype", explode(col("genotypes")))
        .withColumn("INFO_PG", lit(null).cast(ArrayType(IntegerType)))
    }
  }
}
