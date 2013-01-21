package ru.ksu.niimm.cll.anduin

import org.specs.runner.{JUnitSuiteRunner, JUnit4}
import org.specs.Specification
import com.twitter.scalding._
import org.junit.runner.RunWith
import com.twitter.scalding.Tsv

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class TensorHelperProcessorTest extends JUnit4(TensorHelperProcessorTestSpec)

object TensorHelperProcessorTestSpec extends Specification with TupleConversions {
  val entityList: List[(Int, String)] = List(
    (0, "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"),
    (1, "<http://eprints.rkbexplorer.com/id/caltech/person-1>"),
    (2, "<http://eprints.rkbexplorer.com/id/caltech/person-2>"),
    (3, "<http://eprints.rkbexplorer.com/id/caltech/person-3>")
  )
  val adjacencyList = List(
    // 1st row
    (0, "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>", "<http://eprints.rkbexplorer.com/id/caltech/person-1>"),
    // 2nd row
    (0, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "<http://eprints.rkbexplorer.com/id/caltech/person-2>"),
    // 3rd row
    (1, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"),
    // 4th row
    (1, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"),
    // 5th row
    (1, "<http://eprints.rkbexplorer.com/id/caltech/person-12>", "<http://eprints.rkbexplorer.com/id/caltech/eprints-75192222>")
  )
  "For rows, Tensor helper processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.TensorHelperProcessor").
      arg("input", "inputFile").
      arg("inputEntityList", "inputEntityListFile").
      arg("output", "tensor").
      source(TypedTsv[(Int, String)]("inputEntityListFile"), entityList)
      .source(TypedTsv[(Int, String, String)]("inputFile"), adjacencyList)
      .sink[(Int, Int, Int)](Tsv("tensor")) {
      outputBuffer =>
        "output the correct tensor entries" in {
          outputBuffer.size must_== 3
          outputBuffer(0) mustEqual(0, 0, 1)
          outputBuffer(1) mustEqual(0, 1, 2)
          outputBuffer(2) mustEqual(1, 1, 0)
        }
    }.run.
      finish
  }

}
