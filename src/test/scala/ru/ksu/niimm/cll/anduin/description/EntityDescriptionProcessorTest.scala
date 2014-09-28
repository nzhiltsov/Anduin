package ru.ksu.niimm.cll.anduin.description

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import com.twitter.scalding.TextLine

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class EntityDescriptionProcessorTest extends JUnit4(EntityDescriptionProcessorTestSpec)

object EntityDescriptionProcessorTestSpec extends Specification with TupleConversions {
   "The tool" should {
     JobTest("ru.ksu.niimm.cll.anduin.description.EntityDescriptionProcessor").
       arg("input", "inputFile").
       arg("inputEntities", "inputEntitiesFile").
       arg("output", "outputFile")
       .source(TextLine("inputEntitiesFile"), List(
       ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"),
       ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1>")
     ))
       .source(TextLine("inputFile"), List(
       // 3rd row
       ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
         "<http://www.aktors.org/ontology/portal#label> \"No. 1 RNA researcher\" <http://somecontext.com/1> ."),
       // 1st row
       ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
         "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> " +
         "<http://somecontext.com/1> ."),
       // 2nd row
       ("1", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
         "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> <http://somecontext.com/1> ."),
       // 4th row
       ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
         "<http://www.aktors.org/ontology/portal#is-author-of> <http://eprints.rkbexplorer.com/id/caltech/eprints-7519> <http://somecontext.com/1> ."),
       // 5th row
       ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-2> " +
         "<http://www.aktors.org/ontology/portal#other> <http://eprints.rkbexplorer.com/id/caltech/eprints-7519> <http://somecontext.com/1> .")

     )).
       sink[(Subject, Predicate, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)](Tsv("outputFile")) {
       outputBuffer =>
         "output the correct adjacency list" in {
           outputBuffer.size must_== 3
           outputBuffer(0) mustEqual("<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>",
             "<http://www.aktors.org/ontology/portal#has-author>", "<http://eprints.rkbexplorer.com/id/caltech/person-1>")
           outputBuffer(2) mustEqual("<http://eprints.rkbexplorer.com/id/caltech/person-1>",
             "<http://www.aktors.org/ontology/portal#label>", "\"No. 1 RNA researcher\"")
           outputBuffer(1) mustEqual("<http://eprints.rkbexplorer.com/id/caltech/person-1>",
             "<http://www.aktors.org/ontology/portal#is-author-of>", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>")
         }
     }.run.
       finish
   }
}
