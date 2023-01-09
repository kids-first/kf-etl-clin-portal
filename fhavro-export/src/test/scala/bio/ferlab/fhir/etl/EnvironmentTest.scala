package bio.ferlab.fhir.etl

import bio.ferlab.fhir.etl.model.Environment

import org.scalatest.funsuite.AnyFunSuite

class EnvironmentTest extends AnyFunSuite {

  test("fromString to Environment enum.") {
    assert(Environment.fromString("INCLUDE-DEV") === Environment.INCLUDEDEV)
  }

  test("fromString with lowercase string to Environment enum.") {
    assert(Environment.fromString("include-dev") === Environment.INCLUDEDEV)
  }

  test("fromString with invalid string to Environment enum throws NoSuchElementException") {
    assertThrows[NoSuchElementException](Environment.fromString("TEST"))
  }

  test("toString should be lowercase") {
    assert(Environment.INCLUDEDEV.toString === "include-dev")
  }
}
