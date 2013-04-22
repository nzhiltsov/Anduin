package ru.ksu.niimm.cll.anduin.type

import org.specs.runner.JUnit4
import org.specs.Specification
import com.twitter.scalding.{Tsv, TypedTsv, JobTest, TupleConversions}

/**
 * @author Nikita Zhiltsov 
 */
class EntityTypeFilterProcessorTest extends JUnit4(EntityTypeFilterProcessorSpec)

object EntityTypeFilterProcessorSpec extends Specification with TupleConversions {
  "Entity processor " should {
    JobTest("ru.ksu.niimm.cll.anduin.entitytype.EntityTypeFilterProcessor").
      arg("input", "inputFile").
      arg("inputEntityList", "inputEntityListFile").
      arg("output", "outputFile").
      source(TypedTsv[(Int, String)]("inputEntityListFile"), List(
      // 1st row
      (0, "<http://example.org/2>"),
      // 2nd row
      (1, "<http://example.org/3>")
    )).
      source(TypedTsv[(String, String)]("inputFile"), List(
      // 1st row
      ("<http://example.org/1>", "1,2"),
      // 2nd row
      ("<http://example.org/2>", "2"),
      // 3rd row
      ("<http://example.org/3>", "4,5")
    )).
      sink[(Int, String, String)](Tsv("outputFile")) {
      outputBuffer =>
        "output correct entities with types" in {
          outputBuffer.size must_== 2
          outputBuffer(0)._1 must_== 0
          outputBuffer(0)._2 mustEqual "<http://example.org/2>"
          outputBuffer(0)._3 mustEqual "2"

          outputBuffer(1)._1 must_== 1
          outputBuffer(1)._2 mustEqual "<http://example.org/3>"
          outputBuffer(1)._3 mustEqual "4,5"
        }
    }.run.
      finish
  }
}
