package ru.ksu.niimm.cll.anduin.degree

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding.{TextLine, Tsv, JobTest}

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class NodeDegreeProcessorTest extends JUnit4(NodeDegreeProcessorTestSpec)

object NodeDegreeProcessorTestSpec extends Specification {
  val adjacencyList = List(
    // 1st row
    ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> <http://dbpedia.org/ontology/wikiPageWikiLink> <http://eprints.rkbexplorer.com/id/caltech/person-1> ."),
    // 2nd row
    ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> <http://dbpedia.org/ontology/wikiPageWikiLink> <http://eprints.rkbexplorer.com/id/caltech/person-2> ."),
    // 3rd row
    ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> <http://www.w3.org/2002/07/owl#sameAs> <http://eprints.rkbexplorer.com/id/caltech/eprints-7519> ."),
    // 4th row
    ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1> <http://dbpedia.org/ontology/wikiPageWikiLink> <http://eprints.rkbexplorer.com/id/caltech/eprints-7519> ."),
    // 5th row
    ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-1> <http://www.w3.org/2000/01/rdf-schema#label> \"Person 1\" .")
  )

  "Node degree processor" should {
    "output the correct stats with datatype properties" in {
      JobTest("ru.ksu.niimm.cll.anduin.degree.NodeDegreeProcessor").
        arg("input", "inputFile").
        arg("includeDatatype", "true").
        arg("output", "outputFile").
        source(TextLine("inputFile"), adjacencyList)
        .sink[(String, Int)](Tsv("outputFile")) {
        outputBuffer =>
          outputBuffer.size must_== 3
          outputBuffer mustContain("<http://eprints.rkbexplorer.com/id/caltech/person-1>", 5)
          outputBuffer mustContain("<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>", 3)
      }.run.
        finish
    }
    "output the correct stats without datatype properties" in {
      JobTest("ru.ksu.niimm.cll.anduin.degree.NodeDegreeProcessor").
        arg("input", "inputFile").
        arg("includeDatatype", "false").
        arg("output", "outputFile").
        source(TextLine("inputFile"), adjacencyList)
        .sink[(String, Int)](Tsv("outputFile")) {
        outputBuffer =>
          outputBuffer.size must_== 3
          outputBuffer(0) mustEqual("<http://eprints.rkbexplorer.com/id/caltech/person-1>", 4)
          outputBuffer(1) mustEqual("<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>", 3)
      }.run.
        finish
    }
  }
}


