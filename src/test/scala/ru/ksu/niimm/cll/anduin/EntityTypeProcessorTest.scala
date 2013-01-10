package ru.ksu.niimm.cll.anduin

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding.{TextLine, TypedTsv, JobTest, TupleConversions}
import util.{FixedPathLzoTsv, FixedPathLzoTextLine}
import util.NodeParser._

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class EntityTypeProcessorTest extends JUnit4(EntityTypeProcessorTestSpec)

object EntityTypeProcessorTestSpec extends Specification with TupleConversions {
  "Entity type processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.EntityTypeProcessor").
      arg("input", "inputFile").
      arg("inputTypeList", "inputTypeListFile").
      arg("inputEntityList", "inputEntityListFile").
      arg("output", "outputFile").
      source(TypedTsv[(String, Int)]("inputTypeListFile"), List(
      ("<http://xmlns.com/foaf/0.1/Person>", 0),
      ("<http://rdfs.org/sioc/types#WikiArticle>", 1),
      ("<http://purl.org/rss/1.0/item>", 2)
    ))
      .source(TextLine("inputEntityListFile"), List(
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/person-1>")
    ))
      .source(new FixedPathLzoTextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/rss/1.0/item> " +
        "<http://somecontext.com/1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> <http://somecontext.com/1> ."),
      // 3rd row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://somecontext.com/6> ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdfs.org/sioc/types#WikiArticle> <http://somecontext.com/4> ."),
      // 5th row
      ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdfs.org/sioc/types#WikiArticle> <http://somecontext.com/10> .")
    )).
      sink[(Subject, String)](new FixedPathLzoTsv("outputFile")) {
      outputBuffer =>
        "output the correct entity types" in {
          outputBuffer.size must_== 1
          outputBuffer(0)._1 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-1>"
          outputBuffer(0)._2 mustEqual "1, 0"
        }
    }.run.
      finish
  }
}
