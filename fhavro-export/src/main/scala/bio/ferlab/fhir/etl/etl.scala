package bio.ferlab.fhir

import bio.ferlab.fhir.etl.config.Config
import cats.data.Validated.Invalid
import cats.data.{NonEmptyList, Validated}
import org.slf4j.{Logger, LoggerFactory}

package object etl {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  type ValidationResult[A] = Validated[NonEmptyList[String], A]

  def withConfiguration[T](configuration: Config => ValidationResult[T]): ValidationResult[T] = {
    Config.readConfiguration().andThen(configuration)
  }

  def withLog[T](validationResult: ValidationResult[T]): ValidationResult[T] = {
    try {
      validationResult match {
        case Invalid(NonEmptyList(h, t)) =>
          LOGGER.error(h)
          t.foreach(LOGGER.info)
        case Validated.Valid(_) => LOGGER.info("Success!")
      }
      validationResult
    } catch {
      case e: Exception =>
        LOGGER.error("An exception occurred", e)
        throw e
    }
  }

  def withSystemExit[T](validationResult: ValidationResult[T]): Unit = {
    try {
      validationResult match {
        case Invalid(_) => System.exit(-1)
        case _ => ()
      }
    } catch {
      case _: Exception =>
        System.exit(-1)
    }
  }
}
