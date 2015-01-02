package ru.ksu.niimm.cll.anduin.entitysearch

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding.{Tsv, JobTest, TupleConversions, TypedTsv, TextLine}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.PredicateGroupCodes._

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class N3EntityAttributeWithFilteringProcessorTest extends JUnit4(N3EntityAttributeWithFilteringProcessorTestSpec)

object N3EntityAttributeWithFilteringProcessorTestSpec extends Specification with TupleConversions {
  "The name-like attribute processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.entitysearch.EntityAttributeWithFilteringProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      arg("inputFormat", "n3").
      arg("inputPredicates", "inputPredicatesFile").
      arg("entityNames", "entityNamesFile")
      .source(TypedTsv[(String, String)]("inputPredicatesFile"), List(
      ("0", "<http://www.w3.org/2000/01/rdf-schema#label>"),
      ("1", "<http://www.aktors.org/ontology/portal#value>"),
      ("2", "<http://dbpedia.org/ontology/title>"),
      ("3", "<http://dbpedia.org/ontology/office>"),
      ("4", "<http://purl.org/dc/terms/subject>"),
      ("5", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
      ("6", "<http://www.aktors.org/ontology/portal#redirect>"),
      ("7", DBPEDIA_WIKI_PAGE_WIKI_LINK),
      ("8", "<http://www.aktors.org/ontology/portal#knows>")
    ))
      .source(TypedTsv[(String, String)]("entityNamesFile"), List(
      ("<http://dbpedia.org/resource/Author>", "\"Author\"@en"),
      ("<http://dbpedia.org/resource/Category:American_physicists>", "American physicists"),
      ("<http://dbpedia.org/ontology/Scientist>", "Scientist"),
      ("<http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)>", "Caldwell High School"),
      ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "\"type\"@en"),
      ("<http://www.w3.org/2000/01/rdf-schema#label>", "\"label\"@en"),
      ("<http://www.aktors.org/ontology/portal#redirect>", "redirect"),
      ("<http://www.aktors.org/ontology/portal#has-author>", "has author"),
      ("<http://dbpedia.org/ontology/title>", "title"),
      ("<http://eprints.rkbexplorer.com/id/caltech/person-5>", "")
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
        "<http://www.w3.org/2000/01/rdf-schema#label> \"''' No. 1 RNA researcher 1\"@en ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#redirect> <http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)> ."),
      // 5th row
      ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-2> " +
        "<http://www.aktors.org/ontology/portal#value> \"<body><p>123</p></body>\" ."),
      // 6th row
      ("5", "<http://eprints.rkbexplorer.com/id/caltech/person-2> " +
        "<http://www.aktors.org/ontology/portal#value> \"<body><p>321</p></body>\" ."),
      // 7th row
      ("6", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/2000/01/rdf-schema#label> \"No. 1 RNA researcher 1 Fr\"@fr ."),
      // 8th row
      ("7", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://dbpedia.org/ontology/title> \"Researcher\"@en ."),
      // 9th row
        ("8", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
          "<http://dbpedia.org/ontology/office> <http://dbpedia.org/resource/Author> ."),
      // 10th row
      ("9", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://purl.org/dc/terms/subject> <http://dbpedia.org/resource/Category:American_physicists> ."),
      // 11th row
      ("10", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Scientist> ."),
     // 12th row
      ("11", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://dbpedia.org/ontology/wikiPageWikiLink> <http://dbpedia.org/ontology/Scientist> ."),
    // 13th row
      ("12", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://purl.org/dc/terms/subject> <http://dbpedia.org/resource/Category:US_People> ."),
    // 14th row
      ("13", "<http://eprints.rkbexplorer.com/id/caltech/person-4> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-5> .")
    )).
      sink[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct entity descriptions" in {
          outputBuffer.size must_== 5
          outputBuffer mustContain(NAMES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"''' No. 1 RNA researcher 1\"@en")
          outputBuffer mustContain(ATTRIBUTES, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"321\" \"123\"")
          outputBuffer mustContain(ATTRIBUTES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "title \"Researcher\"@en")
          outputBuffer mustContain(CATEGORIES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "American physicists")
          outputBuffer mustContain(OUTGOING_ENTITY_NAMES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"type\"@en Scientist redirect Caldwell High School \"Author\"@en")
        }
    }.run.
      finish
  }
}

