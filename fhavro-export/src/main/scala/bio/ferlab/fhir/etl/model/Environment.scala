package bio.ferlab.fhir.etl.model

object Environment extends Enumeration {

  type Environment = Value

  val DEV   = Value("dev")
  val QA    = Value("qa")
  val UAT   = Value("uat")
  val PROD  = Value("prod")

  def fromString(s: String): Environment = values.find(_.toString.toUpperCase == s.toUpperCase) match {
    case Some(value) => value
    case _ => throw new NoSuchElementException(s"No value for $s")
  }
}
