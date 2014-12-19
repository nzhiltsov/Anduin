package ru.ksu.niimm.cll.anduin.entitysearch

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class OutgoingLinkProcessorTest extends JUnit4(OutgoingLinkProcessorTestSpec)

object OutgoingLinkProcessorTestSpec extends Specification {
  "The outgoing link processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.entitysearch.OutgoingLinkProcessor").
      arg("inputFirstLevel", "inputFirstLevelFile").
      arg("inputSecondLevel", "inputSecondLevelFile").
      arg("output", "outputFile").
      source(TypedTsv[(String, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)]("inputSecondLevelFile"), List(
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"No. 1 RNA researcher 1\""),
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"<body><p>123</p></body>\""),
      ("0", "<http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)>", "\"Relevant name\"")))
      .source(TypedTsv[(String, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)]("inputFirstLevelFile"), List(
      // 1st row
      ("8", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>", "<http://eprints.rkbexplorer.com/id/caltech/person-1>"),
      // 2nd row
      ("10", "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "<http://eprints.rkbexplorer.com/id/caltech/person-2>"),
      // 4th row
      ("12", "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "<http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)>"),
      // 5th row
      ("13", "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"Person 2\"")
    )).
      sink[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct entity descriptions" in {
          outputBuffer.size must_== 2
          outputBuffer mustContain(1, "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>", "\"No. 1 RNA researcher 1\"")
          outputBuffer mustContain(1, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"Relevant name\"")
        }
    }.run.
      finish
  }
}
