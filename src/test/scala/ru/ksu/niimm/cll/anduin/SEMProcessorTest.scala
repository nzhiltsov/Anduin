package ru.ksu.niimm.cll.anduin

import org.junit.runner.RunWith
import org.specs.Specification
import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.NodeParser._
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
        "<http://www.aktors.org/ontology/portal#label> \"No. 1 RNA researcher\" <http://somecontext.com/1> .")
    )).
      sink[(Int, Subject, Predicate, Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output correct ntuples" in {
          outputBuffer(0)._1 must_== 2
          outputBuffer(0)._2 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"
          outputBuffer(0)._3 mustEqual "<http://www.aktors.org/ontology/portal#has-author>"
          outputBuffer(0)._4 mustEqual "\"No. 1 RNA researcher\""
          outputBuffer(1)._1 must_== 0
          outputBuffer(1)._2 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-1>"
          outputBuffer(1)._3 mustEqual "<http://www.aktors.org/ontology/portal#label>"
          outputBuffer(1)._4 mustEqual "\"No. 1 RNA researcher\""
        }
    }.run.
      finish
  }
}
