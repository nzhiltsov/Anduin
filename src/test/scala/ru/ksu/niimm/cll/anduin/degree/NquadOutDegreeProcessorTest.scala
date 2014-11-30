package ru.ksu.niimm.cll.anduin.degree

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding.{TupleConversions, Tsv, TextLine, JobTest}
import ru.ksu.niimm.cll.anduin.util.NodeParser

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class NquadOutDegreeProcessorTest extends JUnit4(NquadOutDegreeProcessorTestSpec)

object NquadOutDegreeProcessorTestSpec extends Specification with TupleConversions {
  "Out degree processor" should {
    JobTest("ru.ksu.niimm.cll.anduin.degree.OutDegreeProcessor").
      arg("input", "inputFile").
      arg("inputFormat", "nquad").
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
      sink[(NodeParser.Subject, Int)](Tsv("outputFile")) {
      outputBuffer =>
        "output correct ntuples" in {
          outputBuffer.size must_== 2
          outputBuffer(1)._1 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"
          outputBuffer(1)._2 must_== 1
          outputBuffer(0)._1 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-1>"
          outputBuffer(0)._2 must_== 2
        }
    }.run.
      finish
  }
}
