package ru.ksu.niimm.cll.anduin.adjacency

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding.{Tsv, TextLine, JobTest, TupleConversions}

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class PredicateFrequencyProcessorTest extends JUnit4(PredicateFrequencyProcessorTestSpec)

object PredicateFrequencyProcessorTestSpec extends Specification with TupleConversions {
  "The processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.adjacency.PredicateFrequencyProcessor").
      arg("inputFirstEntities", "/test.first.entities.txt").
      arg("inputSecondEntities", "/test.second.entities.txt").
      arg("input", "inputFile").
      arg("output", "outputFile")
      .source(TextLine("inputFile"),
      List(
        // 1st row
        ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
          "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> " +
          "<http://somecontext.com/1> ."),
        // 2nd row
        ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
          "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-2> <http://somecontext.com/1> ."),
        // 3rd row
        ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
          "<http://www.aktors.org/ontology/portal#label> \"No. 1 RNA researcher\" <http://somecontext.com/1> ."),
        // 4th row
        ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
          "<http://www.aktors.org/ontology/portal#sameAs> <http://eprints.rkbexplorer.com/id/caltech/eprints-7519> <http://somecontext.com/1> ."),
        //5th row
        ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
          "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
          "<http://somecontext.com/1> .")
      ))
      .sink[(String, Int)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct predicate frequencies" in {
          outputBuffer.size must_== 2
          outputBuffer(0) mustEqual("<http://www.aktors.org/ontology/portal#has-author>", 2)
          outputBuffer(1) mustEqual("<http://www.aktors.org/ontology/portal#sameAs>", 1)
        }
    }.run.
      finish
  }
}
