package ru.ksu.niimm.cll.anduin.adjacency

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding.{Tsv, TextLine, JobTest, TupleConversions}
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTextLine

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class N3CollectionPredicateFrequencyProcessorTest extends JUnit4(N3TopPredicateFinderProcessorTestSpec)

object N3TopPredicateFinderProcessorTestSpec extends Specification with TupleConversions {
  "The top predicate finder processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.adjacency.CollectionPredicateFrequencyProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      arg("inputFormat", "n3").
      source(TextLine("inputFile"),
        List(
          // 1st row
          ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
            "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> ."),
          // 2nd row
          ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
            "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-2> ."),
          // 3rd row
          ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
            "<http://www.aktors.org/ontology/portal#label> \"No. 1 RNA researcher\" ."),
          // 4th row
          ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
            "<http://www.aktors.org/ontology/portal#label> <http://eprints.rkbexplorer.com/id/caltech/eprints-7519> .")
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
