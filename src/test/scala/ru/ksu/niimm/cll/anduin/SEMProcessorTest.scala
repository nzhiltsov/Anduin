package ru.ksu.niimm.cll.anduin

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.specs.Specification
import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.NodeParser._
import com.twitter.scalding.TextLine

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitRunner])
class SEMProcessorTest extends Specification with TupleConversions {
  "SEM processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.testing.TestingSEMProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(TextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://somecontext.com/1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> <http://somecontext.com/1> ."),
      // 3rd row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#label> \"RNA researcher\" <http://somecontext.com/1> .")
    )).
      sink[(Int, Subject, Predicate, Range)](Tsv("outputFile")) {
      outputBuffer =>
        println(outputBuffer(0))
        println(outputBuffer(2))
        "output correct ntuples" in {
          outputBuffer(0)._1 must_== 2
          outputBuffer(0)._2 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"
          outputBuffer(0)._3 mustEqual "<http://www.aktors.org/ontology/portal#has-author>"
          outputBuffer(0)._4 mustEqual "\"RNA researcher\""
          outputBuffer(2)._1 must_== 0
          outputBuffer(2)._2 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/person-1>"
          outputBuffer(2)._3 mustEqual "<http://www.aktors.org/ontology/portal#label>"
          outputBuffer(2)._4 mustEqual "\"RNA researcher\""
        }
    }.run.
      finish
  }
}
