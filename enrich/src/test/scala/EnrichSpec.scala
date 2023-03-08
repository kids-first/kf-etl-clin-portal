import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EnrichSpec extends AnyFlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  it should "just a placeholder for future tests" in {
    1 equals 1
  }
}

