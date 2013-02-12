package ru.ksu.niimm.cll.anduin.sem

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding.{JobTest, TupleConversions}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import com.twitter.scalding.TextLine
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTsv

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class FirstLevelEntityProcessorTest extends JUnit4(FirstLevelEntityProcessorTestSpec)

object FirstLevelEntityProcessorTestSpec extends Specification with TupleConversions {
  "The first level entity processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.sem.FirstLevelEntityProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(new TextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://somecontext.com/1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> <http://somecontext.com/1> ."),
      // 3rd row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#label> \"No. 1 RNA researcher\t1\" <http://somecontext.com/1> ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-3> " +
        "<http://www.aktors.org/ontology/portal#redirect> <http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)> <http://somecontext.com/4> ."),
      // 5th row
      ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-2> " +
        "<http://www.aktors.org/ontology/portal#value> \"<body><p>123</p></body>\" <http://somecontext.com/4> .")
    )).
      sink[(Context, Subject, Predicate, Range)](new FixedPathLzoTsv("outputFile")) {
      outputBuffer =>
        "output the correct entity descriptions" in {
          outputBuffer.size must_== 5
          outputBuffer mustContain("<http://somecontext.com/1>","<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>",
            "<http://www.aktors.org/ontology/portal#has-author>", "<http://eprints.rkbexplorer.com/id/caltech/person-1>")
          outputBuffer mustContain("<http://somecontext.com/1>","<http://eprints.rkbexplorer.com/id/caltech/person-1>",
            "<http://www.aktors.org/ontology/portal#knows>", "<http://eprints.rkbexplorer.com/id/caltech/person-2>")
          outputBuffer mustContain("<http://somecontext.com/1>","<http://eprints.rkbexplorer.com/id/caltech/person-1>",
            "<http://www.aktors.org/ontology/portal#label>", "\"No. 1 RNA researcher 1\"")
          outputBuffer mustContain("<http://somecontext.com/4>","<http://eprints.rkbexplorer.com/id/caltech/person-3>",
            "<http://www.aktors.org/ontology/portal#redirect>", "<http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)>")
          outputBuffer mustContain("<http://somecontext.com/4>","<http://eprints.rkbexplorer.com/id/caltech/person-2>",
            "<http://www.aktors.org/ontology/portal#value>", "\"<body><p>123</p></body>\"")
        }
    }.run.
      finish
  }
}
