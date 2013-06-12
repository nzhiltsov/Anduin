package ru.ksu.niimm.cll.anduin.entitytype

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding._
import com.twitter.scalding.Tsv

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class EntityTypeProcessorTest extends JUnit4(EntityTypeProcessorTestSpec)

object EntityTypeProcessorTestSpec extends Specification with TupleConversions {
  "Entity entitytype processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.entitytype.EntityTypeProcessor").
      arg("input", "inputFile").
      arg("inputTermEntityPairs", "inputTermEntityPairsFile").
      arg("inputTypeList", "inputTypeListFile").
      arg("output", "outputFile").
      source(TypedTsv[(String, String)]("inputTypeListFile"), List(
      ("<http://xmlns.com/foaf/0.1/Person>", "0"),
      ("<http://rdfs.org/sioc/types#WikiArticle>", "1"),
      ("<http://purl.org/rss/1.0/item>", "2")
    ))
      .source(TypedTsv[(String, String)]("inputTermEntityPairsFile"), List(
      ("person", "<http://eprints.rkbexplorer.com/id/caltech/person-1>"),
      ("article", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>")
    ))
      .source(TextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#entitytype> <http://purl.org/rss/1.0/item> " +
        "<http://somecontext.com/1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> <http://somecontext.com/1> ."),
      // 3rd row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#entitytype> <http://xmlns.com/foaf/0.1/Person> <http://somecontext.com/6> ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#entitytype> <http://rdfs.org/sioc/types#WikiArticle> <http://somecontext.com/4> ."),
      // 5th row
      ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#entitytype> <http://rdfs.org/sioc/types#WikiArticle> <http://somecontext.com/10> ."),
      // 6th row
      ("5", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#entitytype> <http://rdfs.org/sioc/types#MissingType> <http://somecontext.com/10> .")
    )).
      sink[(String, Int, Int)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct entity types" in {
          outputBuffer.size must_== 3
          outputBuffer mustContain("person", 0, 1)
          outputBuffer mustContain("person", 1, 1)
          outputBuffer mustContain("article", 2, 1)
        }
    }.run.
      finish
  }
}
