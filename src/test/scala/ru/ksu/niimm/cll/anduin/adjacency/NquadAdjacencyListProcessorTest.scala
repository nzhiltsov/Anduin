package ru.ksu.niimm.cll.anduin.adjacency

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding._
import com.twitter.scalding.TextLine
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTextLine

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class NquadAdjacencyListProcessorTest extends JUnit4(N3AdjacencyListProcessorTestSpec)

object NquadAdjacencyListProcessorTestSpec extends Specification with TupleConversions {
  "Adjacency list processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.adjacency.AdjacencyListProcessor").
      arg("input", "inputFile").
      arg("inputPredicateList", "inputPredicateListFile").
      arg("inputFormat", "nquad").
      arg("output", "outputFile")
      .source(TypedTsv[(String, String)]("inputPredicateListFile"), List(
      ("0", "<http://www.aktors.org/ontology/portal#has-author>"),
      ("2", "<http://www.aktors.org/ontology/portal#label>"),
      ("1", "<http://www.aktors.org/ontology/portal#knows>")
    ))
      .source(TextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://somecontext.com/1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> <http://somecontext.com/1> ."),
      // 3rd row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#label> \"No. 1 RNA researcher\" <http://somecontext.com/1> ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#label> <http://eprints.rkbexplorer.com/id/caltech/eprints-7519> <http://somecontext.com/1> ."),
      // 5th row
      ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#other> <http://eprints.rkbexplorer.com/id/caltech/eprints-7519> <http://somecontext.com/1> .")

    )).
      sink[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct adjacency list" in {
          outputBuffer.size must_== 2
          outputBuffer mustContain(0, "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>", "<http://eprints.rkbexplorer.com/id/caltech/person-1>")
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>")
        }
    }.run.
      finish
  }
}