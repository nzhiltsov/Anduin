package ru.ksu.niimm.cll.anduin

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding.{TextLine, JobTest, TupleConversions}
import util.FixedPathLzoTsv

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class EntityTypeFrequencyProcessorTest extends JUnit4(TopClassFinderProcessorTestSpec)

object TopClassFinderProcessorTestSpec extends Specification with TupleConversions {
  "The top class finder processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.EntityTypeFrequencyProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(TextLine("inputFile"),
      List(
        // 1st row
        ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.aktors.org/ontology/portal#Article-Reference> " +
          "<http://somecontext.com/1> ."),
        // 2nd row
        ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://somecontext.com/1> ."),
        // 3rd row
        ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> \"No. 1 RNA researcher\" <http://somecontext.com/1> ."),
        // 4th row
        ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://somecontext.com/2> ."),
        // 5th row
        ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-133> " +
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://somecontext.com/2> .")
      ))
      .sink[(String, Int)](new FixedPathLzoTsv("outputFile")) {
      outputBuffer =>
        "output the correct entity type frequencies" in {
          outputBuffer.size must_== 2
          outputBuffer(0) mustEqual("<http://xmlns.com/foaf/0.1/Person>", 2)
          outputBuffer(1) mustEqual("<http://www.aktors.org/ontology/portal#Article-Reference>", 1)
        }
    }.run.
      finish
  }
}
