package ru.ksu.niimm.cll.anduin.entitysearch

import org.specs.runner.JUnit4
import org.specs.Specification
import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import com.twitter.scalding.TextLine
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTextLine

/**
 * @author Nikita Zhiltsov 
 */
class N3IdentityLinkProcessorTest extends JUnit4(N3IdentityLinkProcessorTestSpec)

object N3IdentityLinkProcessorTestSpec extends Specification with TupleConversions {
  "The identity link processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.entitysearch.IdentityLinkProcessor").
      arg("inputEntityAttributes", "inputEntityAttributesFile").
      arg("inputGraph", "inputGraphFile").
      arg("inputFormat", "n3").
      arg("output", "outputFile")
      .source(TypedTsv[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)]("inputEntityAttributesFile"), List(
      (0, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"Primary person 1 name\""),
      (1, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"Other attributes of person 1\""),
      (0, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"Primary person 2 name\""),
      (1, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"Other attributes of person 2\""),
      (0, "<http://eprints.rkbexplorer.com/id/caltech/person-3>", "\"Primary person 3 name\""),
      (1, "<http://eprints.rkbexplorer.com/id/caltech/person-5>", "\"Other attributes of person 5\""),
      (1, "<http://eprints.rkbexplorer.com/id/caltech/person-6>", "\"Other attributes of person 6\"")
    ))
      .source(new FixedPathLzoTextLine("inputGraphFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> <http://www.aktors.org/ontology/portal#has-author>" +
        " <http://eprints.rkbexplorer.com/id/caltech/person-1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> <http://www.w3.org/2002/07/owl#sameAs> " +
        "<http://eprints.rkbexplorer.com/id/caltech/person-2> ."),
      // 3d row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-3> <http://www.w3.org/2002/07/owl#sameAs> " +
        "<http://eprints.rkbexplorer.com/id/caltech/person-1> ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1> <http://www.w3.org/2002/07/owl#sameAs> " +
        "<http://eprints.rkbexplorer.com/id/caltech/person-4> ."),
      // 5th row
      ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-5> <http://dbpedia.org/property/disambiguates> " +
        "<http://eprints.rkbexplorer.com/id/caltech/person-1> ."),
      // 6th row
      ("5", "<http://eprints.rkbexplorer.com/id/caltech/person-1> <http://dbpedia.org/property/redirect> " +
        "<http://eprints.rkbexplorer.com/id/caltech/person-6> .")
    )).
      sink[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct entity descriptions" in {
          outputBuffer.size must_== 11
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"Primary person 2 name\"")
          outputBuffer mustContain(3, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"Other attributes of person 2\"")
          outputBuffer mustContain(3, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"Other attributes of person 5\"")
          outputBuffer mustContain(3, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"Other attributes of person 6\"")
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"Primary person 3 name\"")
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/person-3>", "\"Primary person 1 name\"")
          outputBuffer mustContain(3, "<http://eprints.rkbexplorer.com/id/caltech/person-3>", "\"Other attributes of person 1\"")
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"Primary person 1 name\"")
          outputBuffer mustContain(3, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"Other attributes of person 1\"")
          outputBuffer mustContain(2, "<http://eprints.rkbexplorer.com/id/caltech/person-4>", "\"Primary person 1 name\"")
          outputBuffer mustContain(3, "<http://eprints.rkbexplorer.com/id/caltech/person-4>", "\"Other attributes of person 1\"")
        }
    }.run.
      finish
  }
}