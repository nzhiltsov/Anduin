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
  }
}
