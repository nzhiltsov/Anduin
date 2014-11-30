package ru.ksu.niimm.cll.anduin.degree

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding.{Tsv, TypedTsv, JobTest}
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTsv

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class NodeDegreeProcessorTest extends JUnit4(NodeDegreeProcessorTest)

object NodeDegreeProcessorTest extends Specification {
  val adjacencyList = List(
    // 1st row
    ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>", "<http://eprints.rkbexplorer.com/id/caltech/person-1>"),
    // 2nd row
    ("0", "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "<http://eprints.rkbexplorer.com/id/caltech/person-2>"),
    // 3rd row
    ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"),
    // 4th row
    ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"),
    // 5th row
    ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-12>", "<http://eprints.rkbexplorer.com/id/caltech/eprints-75192222>")
  )

  "Node degree processor" should {
    JobTest("ru.ksu.niimm.cll.anduin.degree.NodeDegreeProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(TypedTsv[(String, String, String)]("inputFile"), adjacencyList)
      .sink[(String, Int)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct stats" in {
          outputBuffer.size must_== 5
          outputBuffer mustContain("<http://eprints.rkbexplorer.com/id/caltech/person-1>", 4)
          outputBuffer mustContain("<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>", 3)
        }
    }.run.
      finish
  }
}


