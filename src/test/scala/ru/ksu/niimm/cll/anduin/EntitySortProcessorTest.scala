package ru.ksu.niimm.cll.anduin

import org.specs.runner.JUnit4
import org.specs.Specification
import com.twitter.scalding.{TupleConversions, Tsv, TextLine, JobTest}
import util.NodeParser._

/**
 * @author Nikita Zhiltsov 
 */
class EntitySortProcessorTest extends JUnit4(EntitySortProcessorTestSpec)

object EntitySortProcessorTestSpec extends Specification with TupleConversions {
  "Entity processor " should {
    JobTest("ru.ksu.niimm.cll.anduin.EntitySortProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(TextLine("inputFile"), List(
      // 1st row
      ("0", "1\t<http://example.org/Dvaleriafraschetti>\t<http://xmlns.com/foaf/0.1/nick>\t\"valeria fraschetti\""),
      // 2nd row
      ("1", "0\t<http://example.org/Dvaleriafraschetti>\t<http://xmlns.com/foaf/0.1/nick>\t\"valeria fraschetti\""),
      // 3rd row
      ("2", "0\t<http://example.org/Dvaleriafraschetti>\t<http://xmlns.com/foaf/0.1/gender>\t\"Female\""),
      // 4th row
      ("3", "0\t<http://example.org/Dvaleriafraschettz>\t<http://xmlns.com/foaf/0.1/gender>\t\"Female\"")
    )).
      sink[(Int, Subject, Predicate, Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output correct ntuples" in {
          outputBuffer.size must_== 4
          outputBuffer(0)._2 mustEqual "<http://example.org/Dvaleriafraschetti>"
          outputBuffer(0)._1 must_== 1
          outputBuffer(0)._3 mustEqual "<http://xmlns.com/foaf/0.1/nick>"
          outputBuffer(1)._2 mustEqual "<http://example.org/Dvaleriafraschetti>"
          outputBuffer(1)._1 must_== 0
          outputBuffer(2)._2 mustEqual "<http://example.org/Dvaleriafraschetti>"
          outputBuffer(2)._1 must_== 0
          outputBuffer(3)._2 mustEqual "<http://example.org/Dvaleriafraschettz>"
          outputBuffer(3)._1 must_== 0
        }
    }.run.
      finish
  }
}
