package ru.ksu.niimm.cll.anduin.entitysearch

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import com.twitter.scalding.Tsv
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTextLine

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class OutgoingLinkProcessorTest extends JUnit4(OutgoingLinkProcessorTestSpec)

object OutgoingLinkProcessorTestSpec extends Specification with TupleConversions {
  "The outgoing link processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.entitysearch.OutgoingLinkProcessor").
      arg("inputFirstLevel", "inputFirstLevelFile").
      arg("inputSecondLevel", "inputSecondLevelFile").
      arg("output", "outputFile").
      source(TypedTsv[(String, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)]("inputSecondLevelFile"), List(
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"No. 1 RNA researcher 1\""),
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"<body><p>123</p></body>\""),
      ("0", "<http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)>", "\"Relevant name\""),
      ("0", "<http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)>", "\"Other relevant name\"")))
      .source(new FixedPathLzoTextLine("inputFirstLevelFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> ."),
      // 4th row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-2> " +
        "<http://www.aktors.org/ontology/portal#redirect> <http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)> ."),
      // 5th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-2> <http://www.aktors.org/ontology/portal#label> \"Person 2\" .")
    )).
      sink[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct entity descriptions" in {
          outputBuffer.size must_== 4
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>", "\"No. 1 RNA researcher 1\"")
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"Relevant name\"")
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"Other relevant name\"")
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"person\"")
        }
    }.run.
      finish
  }
}
