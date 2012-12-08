package ru.ksu.niimm.cll.anduin

import org.specs._
import com.twitter.scalding.{Tsv, TextLine, TupleConversions, JobTest}
import ru.ksu.niimm.cll.anduin.NodeParser._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitRunner])
class DocumentEntityProcessorTest extends Specification with TupleConversions {
  "DE processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.testing.TestingDocumentEntityProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(TextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://somecontext.com/1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#profession> \"RNA researcher\" <http://somecontext.com/1> .")
    )).
      sink[(Subject, Predicate, Range)](Tsv("outputFile")) {
      outputBuffer =>
        println(outputBuffer(0))
        "output correct ntuples" in {
          outputBuffer(0)._1 mustEqual "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"
          outputBuffer(0)._2 mustEqual "<http://www.aktors.org/ontology/portal#has-author>"
          outputBuffer(0)._3 mustEqual "\"RNA researcher\""
        }
    }.run.
      finish
  }
}
