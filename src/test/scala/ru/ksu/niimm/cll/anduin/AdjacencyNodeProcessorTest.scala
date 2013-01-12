package ru.ksu.niimm.cll.anduin

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding._

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class AdjacencyNodeProcessorTest extends JUnit4(AdjacencyNodeProcessorTestSpec)

object AdjacencyNodeProcessorTestSpec extends Specification with TupleConversions {
  "Adjacency node processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.AdjacencyNodeProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(TypedTsv[(Int, String, String)]("inputFile"), List(
      (0, "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>", "<http://eprints.rkbexplorer.com/id/caltech/person-1>"),
      (0, "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>", "<http://eprints.rkbexplorer.com/id/caltech/person-2>"),
      (0, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "<http://eprints.rkbexplorer.com/id/caltech/person-2>")
    )).
      sink[String](new TextLine("outputFile")) {
      outputBuffer =>
        "output the correct entity list" in {
          outputBuffer.size must_== 3
          outputBuffer(0) mustEqual "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"
          outputBuffer(1) mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-1>"
          outputBuffer(2) mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-2>"
        }
    }.run.
      finish
  }
}
