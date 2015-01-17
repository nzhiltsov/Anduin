package ru.ksu.niimm.cll.anduin.degree

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding.{Tsv, TextLine, JobTest}
import ru.ksu.niimm.cll.anduin.util.NodeParser

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class N3OutObjectLinkDegreeProcessorTest extends JUnit4(N3OutObjectLinkDegreeProcessorTestSpec)

object N3OutObjectLinkDegreeProcessorTestSpec extends Specification {
  "Out degree processor" should {
    JobTest("ru.ksu.niimm.cll.anduin.degree.OutDegreeProcessor").
      arg("input", "inputFile").
      arg("inputFormat", "n3").
      arg("includeDatatype", "false").
      arg("output", "outputFile").
      source(TextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> ."),
      // 3rd row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#label> \"No. 1 RNA researcher\" ."),
     // 4th row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#refersTo> <http://eprints.rkbexplorer.com/id/caltech/person-3> .")
    )).
      sink[(NodeParser.Subject, Int)](Tsv("outputFile")) {
      outputBuffer =>
        "output correct ntuples" in {
          outputBuffer.size must_== 2
          outputBuffer(0)._1 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"
          outputBuffer(0)._2 must_== 2
          outputBuffer(1)._1 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-1>"
          outputBuffer(1)._2 must_== 1
        }
    }.run.
      finish
  }
}

