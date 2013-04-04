package ru.ksu.niimm.cll.anduin.util

import org.junit.runner.RunWith
import org.specs.Specification
import NodeParser._
import org.scalatest.junit.JUnitRunner

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitRunner])
object NodeParserTest extends Specification {
  "Node parser" should {
    "extract nodes" in {
      val line = "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-volume> \"72\" <http://somecontext.com/1> ."
      extractNodes(line) match {
        case (c, s, p, o) => {
          c mustEqual "<http://somecontext.com/1>"
          s mustEqual "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"
          p mustEqual "<http://www.aktors.org/ontology/portal#has-volume>"
          o mustEqual "\"72\""
        }
        case _ => fail("didn't extract nodes")
      }
    }
    "extract nodes including blank ones" in {
      val line = "_:p1 <http://www.aktors.org/ontology/portal#birthDate> _:p2 <http://somecontext.com/1> ."
      extractNodes(line) match {
        case (c, s, p, o) => {
          c mustEqual "<http://somecontext.com/1>"
          s mustEqual "_:p1"
          p mustEqual "<http://www.aktors.org/ontology/portal#birthDate>"
          o mustEqual "_:p2"
        }
        case _ => fail("didn't extract nodes")
      }
    }

    "extract nodes from ntuple" in {
      val line = "<http://someurl> <http://www.aktors.org/ontology/portal#has-author>\t" +
        "\"RNA researcher\" \"Top RNA researcher\" _:p1"
      extractNodesFromNTuple(line) match {
        case (s, p, o) => {
          s mustEqual "<http://someurl>"
          p mustEqual "<http://www.aktors.org/ontology/portal#has-author>"
          o mustEqual "\"RNA researcher\" \"Top RNA researcher\" _:p1"
        }
        case _ => fail("didn't extract nodes")
      }
    }

    "detect name-like attributes" in {
      val positivePredicates = List("http://xmlns.com/foaf/0.1/givenName",
        "http://dbpedia.org/ontology/birthName",
        "http://dbpedia.org/property/birthName",
        "http://dbpedia.org/property/name",
        "http://www.w3.org/2000/01/rdf-schema#label",
      "http://xmlns.com/foaf/0.1/surname",
      "http://dbpedia.org/property/name",
      "http://xmlns.com/foaf/0.1/accountName",
      "http://dbpedia.org/ontology/formerName",
      "http://dbpedia.org/property/formerNames",
      "http://dbpedia.org/property/title",
      "http://dbpedia.org/property/englishTitle",
      "http://xmlns.com/foaf/0.1/nick",
      "http://purl.org/dc/elements/1.1/title")
      positivePredicates.forall(isNamePredicate) must_== true
      val negativePredicates =List("http://www.w3.org/2000/01/rdf-schema#comment",
        "http://dbpedia.org/property/caption",
        "http://dbpedia.org/ontology/abstract")
      negativePredicates.exists(isNamePredicate) must_== false
    }

  }

}
