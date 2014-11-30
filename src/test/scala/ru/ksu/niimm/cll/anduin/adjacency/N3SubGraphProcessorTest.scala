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
class N3SubGraphProcessorTest extends JUnit4(N3SubGraphProcessorTestSpec)

object N3SubGraphProcessorTestSpec extends Specification with TupleConversions {
  "Subgraph processor" should {
    JobTest("ru.ksu.niimm.cll.anduin.adjacency.SubGraphProcessor").
      arg("input", "inputFile").
      arg("inputEntities", "inputEntitiesFile").
      arg("inputFormat", "n3").
      arg("output", "outputFile").
      source(TextLine("inputEntitiesFile"), List(
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"),
      ("1", "<http://example.com/2>"),
      ("2", "<http://example.com/3>"),
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1>"),
      ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-2>")
    ))
      .source(TextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> ."),
      // 3rd row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#label> <http://example.com/3> ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-3> " +
        "<http://www.aktors.org/ontology/portal#redirect> <http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)> ."),
      // 5th row
      ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-2> " +
        "<http://www.aktors.org/ontology/portal#value> \"<body><p>123</p></body>\"@en ."),
      // 6th row
      ("5", "<http://eprints.rkbexplorer.com/id/caltech/person-2> " +
        "<http://www.aktors.org/ontology/portal#name> <http://eprints.rkbexplorer.com/id/caltech/person-1> .")
    )).
      sink[(String, String, String)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct subgraph" in {
          outputBuffer.size must_== 4
          outputBuffer mustContain("<http://eprints.rkbexplorer.com/id/caltech/person-2>",
            "<http://www.aktors.org/ontology/portal#name>",
            "<http://eprints.rkbexplorer.com/id/caltech/person-1>")
          outputBuffer mustContain("<http://eprints.rkbexplorer.com/id/caltech/person-1>",
            "<http://www.aktors.org/ontology/portal#label>",
            "<http://example.com/3>")
          outputBuffer mustContain("<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>",
            "<http://www.aktors.org/ontology/portal#has-author>",
            "<http://eprints.rkbexplorer.com/id/caltech/person-1>")
          outputBuffer mustContain("<http://eprints.rkbexplorer.com/id/caltech/person-1>",
            "<http://www.aktors.org/ontology/portal#knows>",
            "<http://eprints.rkbexplorer.com/id/caltech/person-2>")
        }
    }.run.
      finish
  }
}