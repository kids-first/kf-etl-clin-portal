package bio.ferlab.etl.mainutils

import mainargs.{ParserForClass, arg}

case class Studies(@arg(name = "study-id", short = 'i', doc = "Study Id") private val id: Seq[String]) {
  val ids: List[String] = id.flatMap(_.split(",")).toList
}

object Studies{
  implicit def configParser: ParserForClass[Studies] = ParserForClass[Studies]
}
