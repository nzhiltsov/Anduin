package ru.ksu.niimm.cll.anduin.entitysearch

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTextLine
import ru.ksu.niimm.cll.anduin.util.PredicateGroupCodes._


/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class NquadEntityAttributeWithFilteringProcessorTest extends JUnit4(NquadEntityAttributeWithFilteringProcessorTestSpec)

object NquadEntityAttributeWithFilteringProcessorTestSpec extends Specification with TupleConversions {
  "The name-like attribute processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.entitysearch.EntityAttributeWithFilteringProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      arg("inputFormat", "nquad").
      arg("inputPredicates", "inputPredicatesFile").
      arg("entityNames", "entityNamesFile")
      .source(TypedTsv[(String, String)]("inputPredicatesFile"), List(
      ("0", "<http://www.w3.org/2000/01/rdf-schema#label>"),
      ("1", "<http://www.aktors.org/ontology/portal#value>"),
      ("2", "<http://dbpedia.org/ontology/title>"),
      ("3", "<http://dbpedia.org/ontology/office>")
    ))
      .source(TypedTsv[(String, String)]("entityNamesFile"), List(
      ("<http://dbpedia.org/resource/Author>", "\"Author\"@en")
    ))
      .source(TextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> <http://context.com/1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> <http://context.com/1> ."),
      // 3rd row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/2000/01/rdf-schema#label> \"No. 1 RNA researcher 1\" <http://context.com/1> ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-3> " +
        "<http://www.aktors.org/ontology/portal#redirect> <http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)> <http://context.com/1> ."),
      // 5th row
      ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-2> " +
        "<http://www.aktors.org/ontology/portal#value> \"<body><p>123</p></body>\" <http://context.com/1> ."),
      // 6th row
      ("5", "<http://eprints.rkbexplorer.com/id/caltech/person-2> " +
        "<http://www.aktors.org/ontology/portal#value> \"<body><p>321</p></body>\" <http://context.com/1> ."),
      // 7th row
      ("6", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/2000/01/rdf-schema#label> \"No. 1 RNA researcher 1\"@fr <http://context.com/1> ."),
      // 8th row
      ("7", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://dbpedia.org/ontology/title> \"Researcher\"@en <http://context.com/1> ."),
        // 9th row
        ("8", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
          "<http://dbpedia.org/ontology/office> <http://dbpedia.org/resource/Author> <http://context.com/1> .")
    )).
      sink[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct entity descriptions" in {
          outputBuffer.size must_== 3
          outputBuffer mustContain(NAMES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"No. 1 RNA researcher 1\"")
          outputBuffer mustContain(ATTRIBUTES, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"321\" \"123\"")
          outputBuffer mustContain(TITLES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"Author\"@en \"Researcher\"@en")
        }
    }.run.
      finish
  }
}

