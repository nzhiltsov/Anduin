package ru.ksu.niimm.cll.anduin.adjacency

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTextLine

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class NquadCollectionPredicateFrequencyProcessorTest extends JUnit4(NquadTopPredicateFinderProcessorTestSpec)

object NquadTopPredicateFinderProcessorTestSpec extends Specification with TupleConversions {
  "The top predicate finder processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.adjacency.CollectionPredicateFrequencyProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      arg("inputFormat", "nquad").
      source(TextLine("inputFile"),
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
          "<http://www.aktors.org/ontology/portal#label> <http://eprints.rkbexplorer.com/id/caltech/eprints-7519> <http://somecontext.com/1> .")
      ))
      .sink[(String, Int)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct top predicates" in {
          outputBuffer.size must_== 2
          outputBuffer(0) mustEqual("<http://www.aktors.org/ontology/portal#has-author>", 2)
          outputBuffer(1) mustEqual("<http://www.aktors.org/ontology/portal#label>", 1)
        }
    }.run.
      finish
  }

}
