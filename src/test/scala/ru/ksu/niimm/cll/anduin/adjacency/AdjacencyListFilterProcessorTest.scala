package ru.ksu.niimm.cll.anduin.adjacency

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser.Subject
import ru.ksu.niimm.cll.anduin.util.NodeParser.Range
import com.twitter.scalding.Tsv
import com.twitter.scalding.TextLine

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class AdjacencyListFilterProcessorTest extends JUnit4(AdjacencyListFilterProcessorTestSpec)

object AdjacencyListFilterProcessorTestSpec extends Specification with TupleConversions {
  "Adjacency list filter processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.adjacency.AdjacencyListFilterProcessor").
      arg("input", "inputFile").
      arg("inputEntities", "inputEntitiesFile").
      arg("inputPredicates", "inputPredicateFile").
      arg("output", "outputFile")
      .source(TextLine("inputEntitiesFile"), List(
      ("0", "http://example.com/1"),
      ("1", "http://example.com/2"),
      ("2", "http://example.com/3")
    ))
      .source(TypedTsv[(String, String)]("inputPredicateFile"), List(
      ("0", "http://example.com/predicate/1")
    ))
      .source(TypedTsv[(String, String, String)]("inputFile"), List(
      ("0", "http://example.com/1", "http://example.com/10"),
      ("2", "http://example.com/34", "http://example.com/12"),
      ("3", "http://example.com/34", "http://example.com/2"),
      ("0", "http://example.com/1", "http://example.com/2")
    ))
      .sink[(Int, Subject, Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct adjacency list" in {
          outputBuffer.size must_== 2
          outputBuffer mustContain (0, "http://example.com/1", "http://example.com/10")
          outputBuffer mustContain (0, "http://example.com/1", "http://example.com/2")
        }
    }.run.
      finish
  }
}
