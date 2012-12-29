package ru.ksu.niimm.cll.anduin

import org.junit.runner.RunWith
import org.specs.Specification
import com.twitter.scalding._
import util.NodeParser
import NodeParser._
import com.twitter.scalding.TextLine
import org.specs.runner.{JUnit4, JUnitSuiteRunner}

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class SEMProcessorTest extends JUnit4(SEMProcessorTestSpec)

object SEMProcessorTestSpec extends Specification with TupleConversions {
  "SEM processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.SEMProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(TextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://somecontext.com/1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> <http://somecontext.com/1> ."),
      // 3rd row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#label> \"No. 1 RNA researcher\" <http://somecontext.com/1> ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-3> " +
        "<http://www.aktors.org/ontology/portal#redirect> <http://eprints.rkbexplorer.com/id/caltech/person-4> <http://somecontext.com/4> .")
    )).
      sink[(Int, Subject, Predicate, Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output correct ntuples" in {
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>",
            "<http://www.aktors.org/ontology/portal#has-author>", "\"No. 1 RNA researcher\"")
          outputBuffer mustContain(0, "<http://eprints.rkbexplorer.com/id/caltech/person-1>",
            "<http://www.aktors.org/ontology/portal#label>", "\"No. 1 RNA researcher\"")
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/person-3>",
            "<http://www.aktors.org/ontology/portal#redirect>", "<http://eprints.rkbexplorer.com/id/caltech/person-4>")
        }
    }.run.
      finish
  }
}
