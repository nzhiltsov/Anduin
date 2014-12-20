package ru.ksu.niimm.cll.anduin.entitysearch

import org.specs.runner.JUnit4
import org.specs.Specification
import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import com.twitter.scalding.TextLine
import ru.ksu.niimm.cll.anduin.util.PredicateGroupCodes._
/**
 * @author Nikita Zhiltsov 
 */
class NquadIdentityLinkProcessorTest extends JUnit4(NquadIdentityLinkProcessorTestSpec)

object NquadIdentityLinkProcessorTestSpec extends Specification with TupleConversions {
  "The identity link processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.entitysearch.IdentityLinkProcessor").
      arg("input", "inputFile").
      arg("inputFormat", "nquad").
      arg("entityNames", "entityNamesFile").
      arg("output", "outputFile")
      .source(TypedTsv[(String, String)]("entityNamesFile"), List(
      ("<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"person 1\""),
      ("<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"person 2\""),
      ("<http://eprints.rkbexplorer.com/id/caltech/person-3>", "\"person 3\""),
      ("<http://eprints.rkbexplorer.com/id/caltech/person-6>", "\"person 6\"")
    ))
      .source(TextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> <http://www.aktors.org/ontology/portal#has-author>" +
        " <http://eprints.rkbexplorer.com/id/caltech/person-1> <http://context.com/1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> <http://www.w3.org/2002/07/owl#sameAs> " +
        "<http://eprints.rkbexplorer.com/id/caltech/person-2> <http://context.com/1> ."),
      // 3d row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-3> <http://www.w3.org/2002/07/owl#sameAs> " +
        "<http://eprints.rkbexplorer.com/id/caltech/person-1> <http://context.com/1> ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1> <http://www.w3.org/2002/07/owl#sameAs> " +
        "<http://eprints.rkbexplorer.com/id/caltech/person-4> <http://context.com/1> ."),
      // 5th row
      ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-5> <http://dbpedia.org/ontology/wikiPageDisambiguates> " +
        "<http://eprints.rkbexplorer.com/id/caltech/person-1> <http://context.com/1> ."),
      // 6th row
      ("5", "<http://eprints.rkbexplorer.com/id/caltech/person-6> <http://dbpedia.org/ontology/wikiPageRedirects> " +
        "<http://eprints.rkbexplorer.com/id/caltech/person-1> <http://context.com/1> .")
    )).
      sink[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct entity descriptions" in {
          outputBuffer.size must_== 5
          //          outputBuffer mustContain(SIMILAR_ENTITY_NAMES,
          //            "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"person 3\" \"person 2\" \"person 6\"")
          outputBuffer mustContain(SIMILAR_ENTITY_NAMES,
            "<http://eprints.rkbexplorer.com/id/caltech/person-3>", "\"person 1\"")
          outputBuffer mustContain(SIMILAR_ENTITY_NAMES,
            "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"person 1\"")
          outputBuffer mustContain(SIMILAR_ENTITY_NAMES,
            "<http://eprints.rkbexplorer.com/id/caltech/person-4>", "\"person 1\"")
          outputBuffer mustContain(SIMILAR_ENTITY_NAMES,
            "<http://eprints.rkbexplorer.com/id/caltech/person-5>", "\"person 1\"")

          outputBuffer mustExist(isValidTuple)
        }
    }.run.
      finish
  }

  def isValidTuple: ((Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)) => Boolean = tuple =>
    tuple._1 == SIMILAR_ENTITY_NAMES &&
      tuple._2.equals("<http://eprints.rkbexplorer.com/id/caltech/person-1>") && tuple._3.split("\"").contains("person 3") &&
      tuple._3.split("\"").contains("person 2") && tuple._3.split("\"").contains("person 6")
}