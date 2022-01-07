package bio.ferlab.fhir.etl.model

object Environment extends Enumeration {

  type Environment = Value

  val INCLUDEDEV   = Value("include-dev")
  val KFDRCDEV   = Value("kfdrc-dev")
  val INCLUDEQA    = Value("include-qa")
  val KFDRCQA    = Value("kfdrc-qa")
  val INCLUDEPROD  = Value("include-prd")
  val KFDRCPROD  = Value("kfdrc-prd")

  def fromString(s: String): Environment = values.find(_.toString.toUpperCase == s.toUpperCase) match {
    case Some(value) => value
    case _ => throw new NoSuchElementException(s"No value for $s")
  }
}
