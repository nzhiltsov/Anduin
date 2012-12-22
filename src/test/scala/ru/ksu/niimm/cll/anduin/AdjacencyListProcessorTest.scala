package ru.ksu.niimm.cll.anduin

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding._
import util.NodeParser
import NodeParser._
import com.twitter.scalding.TextLine

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class AdjacencyListProcessorTest extends JUnit4(AdjacencyListProcessorTestSpec)

object AdjacencyListProcessorTestSpec extends Specification with TupleConversions {
  "Adjacency list processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.AdjacencyListProcessor").
      arg("input", "inputFile").
      arg("inputPredicateList", "inputPredicateListFile").
      arg("output", "outputFile").
      source(TypedTsv[(String, Int)]("inputPredicateListFile"), List(
      ("<http://www.aktors.org/ontology/portal#has-author>", 0),
      ("<http://www.aktors.org/ontology/portal#label>", 1),
      ("<http://www.aktors.org/ontology/portal#knows>", 2)
    ))
      .source(TextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://somecontext.com/1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> <http://somecontext.com/1> ."),
      // 3rd row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#label> \"No. 1 RNA researcher\" <http://somecontext.com/1> .")
    )).
      sink[(Int, Subject, Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output correct ntuples" in {
          outputBuffer.size must_== 2
          outputBuffer(0)._1 must_== 0
          outputBuffer(0)._2 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"
          outputBuffer(0)._3 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-1>"
          outputBuffer(1)._1 must_== 2
          outputBuffer(1)._2 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-1>"
          outputBuffer(1)._3 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-2>"
        }
    }.run.
      finish
  }
}