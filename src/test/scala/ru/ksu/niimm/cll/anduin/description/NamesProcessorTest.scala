package ru.ksu.niimm.cll.anduin.description

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import org.specs.Specification

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class NamesProcessorTest extends JUnit4(NamesProcessorTestSpec)

object NamesProcessorTestSpec extends Specification {
  "The names processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.description.NamesProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile")
      .source(TextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> ."),
      // 3rd row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/2000/01/rdf-schema#label> \"No. 1 RNA researcher 1\" ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-3> " +
        "<http://www.aktors.org/ontology/portal#redirect> <http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)> ."),
      // 5th row
      ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-2> " +
        "<http://www.aktors.org/ontology/portal#value> \"<body><p>123</p></body>\" ."),
      // 6th row
      ("5", "<http://eprints.rkbexplorer.com/id/caltech/person-2> " +
        "<http://www.aktors.org/ontology/portal#value> \"<body><p>321</p></body>\" ."),
      // 7th row
      ("6", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/2000/01/rdf-schema#label> \"No. 1 RNA researcher 1\"@fr ."),
      // 8th row
      ("7", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://dbpedia.org/property/name> \"Researcher\"@en .")
    )).
      sink[(Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct entity descriptions" in {
          outputBuffer.size must_== 1
          outputBuffer mustContain("<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"Researcher\"@en \"No. 1 RNA researcher 1\"")
        }
    }.run.
      finish
  }
}
