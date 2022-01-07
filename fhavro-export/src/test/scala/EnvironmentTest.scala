import bio.ferlab.fhir.etl.model.Environment
import org.scalatest.FunSuite

class EnvironmentTest extends FunSuite {

  test("fromString to Environment enum.") {
    assert(Environment.fromString("DEV") === Environment.DEV)
  }

  test("fromString with lowercase string to Environment enum.") {
    assert(Environment.fromString("dev") === Environment.DEV)
  }

  test("fromString with invalid string to Environment enum throws NoSuchElementException") {
    assertThrows[NoSuchElementException](Environment.fromString("TEST"))
  }

  test("toString should be lowercase") {
    assert(Environment.DEV.toString === "dev")
  }
}
