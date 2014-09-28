package ru.ksu.niimm.cll.anduin.entitysearch

import org.junit.runner.RunWith
import org.specs.Specification
import com.twitter.scalding._
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTsv

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class SEMProcessorTest extends JUnit4(SEMProcessorTestSpec)

object SEMProcessorTestSpec extends Specification with TupleConversions {
  "The SEM processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.entitysearch.SEMProcessor").
      arg("inputNameLike", "inputNameLikeFile").
      arg("inputOutgoingLinks", "inputOutgoingLinksFile").
      arg("inputIncomingLinks", "inputIncomingLinksFile").
      arg("output", "outputFile")
      .source(TypedTsv[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)]("inputNameLikeFile"), List(
      (0, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"No. 1 RNA researcher 1\""),
      (1, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"123\"")
    ))
      .source(TypedTsv[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)]("inputOutgoingLinksFile"), List(
      (2, "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>", "\"No. 1 RNA researcher 1\""),
      (2, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "person"),
      (2, "<http://eprints.rkbexplorer.com/id/caltech/person-3>", "Caldwell High School Caldwell Texas"),
      (2, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"Relevant name\"")
    ))
      .source(TypedTsv[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)]("inputIncomingLinksFile"), List(
      (3, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"No. 1 RNA researcher 1\""),
      (3, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "eprints"),
      (3, "<http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)>", "person")
    ))
      .sink[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)](new FixedPathLzoTsv("outputFile")) {
      outputBuffer =>
        "output the correct entity descriptions" in {
          outputBuffer.size must_== 9
          outputBuffer mustContain(0, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"No. 1 RNA researcher 1\"")
          outputBuffer mustContain(1, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"123\"")
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>", "\"No. 1 RNA researcher 1\"")
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "person")
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/person-3>", "Caldwell High School Caldwell Texas")
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"Relevant name\"")
          outputBuffer mustContain(3, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"No. 1 RNA researcher 1\"")
          outputBuffer mustContain(3, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "eprints")
          outputBuffer mustContain(3, "<http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)>", "person")
        }
    }.run.
      finish
  }
}
