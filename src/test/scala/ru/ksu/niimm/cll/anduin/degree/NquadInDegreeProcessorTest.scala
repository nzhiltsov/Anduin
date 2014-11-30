package ru.ksu.niimm.cll.anduin.degree

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding.{Tsv, TextLine, JobTest, TupleConversions}
import ru.ksu.niimm.cll.anduin.util.NodeParser

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class NquadInDegreeProcessorTest extends JUnit4(NquadInDegreeProcessorTestSpec)

object NquadInDegreeProcessorTestSpec extends Specification with TupleConversions {
  "In degree processor" should {
    JobTest("ru.ksu.niimm.cll.anduin.degree.InDegreeProcessor").
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
        "<http://www.aktors.org/ontology/portal#label> \"No. 1 RNA researcher\" <http://somecontext.com/1> ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-4> " +
        "<http://www.aktors.org/ontology/portal#co-author-with> <http://eprints.rkbexplorer.com/id/caltech/person-2> <http://somecontext.com/2>.")
    )).
      sink[(NodeParser.Subject, Int)](Tsv("outputFile")) {
      outputBuffer =>
        "output correct ntuples" in {
          outputBuffer.size must_== 2
          outputBuffer(0)._1 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-2>"
          outputBuffer(0)._2 must_== 2
          outputBuffer(1)._1 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-1>"
          outputBuffer(1)._2 must_== 1
        }
    }.run.
      finish
  }
}

