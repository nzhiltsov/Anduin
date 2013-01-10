package ru.ksu.niimm.cll.anduin

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding.{TextLine, JobTest, TupleConversions}
import util.{FixedPathLzoTsv, FixedPathLzoTextLine}
import util.NodeParser._

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class AdjacencyOfCandidateEntitiesProcessorTest extends JUnit4(AdjacencyOfCandidateEntitiesProcessorTestSpec)

object AdjacencyOfCandidateEntitiesProcessorTestSpec extends Specification with TupleConversions {
  "Adjacency list filter processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.AdjacencyOfCandidateEntitiesProcessor").
      arg("input", "inputFile").
      arg("inputCandidateList", "inputCandidateListFile").
      arg("output", "outputFile").
      source(new FixedPathLzoTextLine("inputCandidateListFile"), List(
      ("0", "http://eprints.rkbexplorer.com/id/caltech/eprints-7519")
    ))
      .source(new TextLine("inputFile"), List(
      // 1st row
      ("0", "0\t<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>\t" +
        "<http://eprints.rkbexplorer.com/id/caltech/person-1>"),
      // 2nd row
      ("1", "0\t<http://eprints.rkbexplorer.com/id/caltech/person-1>\t" +
        "<http://eprints.rkbexplorer.com/id/caltech/person-2>"),
      // 3rd row
      ("2", "2\t<http://eprints.rkbexplorer.com/id/caltech/person-1>\t" +
        "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>")
    )).
      sink[(Int, Subject, Range)](new FixedPathLzoTsv("outputFile")) {
      outputBuffer =>
        "output the correct adjacency list" in {
          outputBuffer.size must_== 2
          outputBuffer(0)._1 must_== 0
          outputBuffer(0)._2 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"
          outputBuffer(0)._3 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-1>"
          outputBuffer(1)._1 must_== 2
          outputBuffer(1)._2 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-1>"
          outputBuffer(1)._3 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"
        }
    }.run.
      finish
  }
}
