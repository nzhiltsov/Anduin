package ru.ksu.niimm.cll.anduin

import org.junit.runner.RunWith
import org.specs.Specification
import ru.ksu.niimm.cll.anduin.NodeParser._
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
  }
}
