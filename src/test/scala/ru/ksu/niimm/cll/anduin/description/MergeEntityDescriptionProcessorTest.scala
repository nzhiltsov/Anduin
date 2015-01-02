package ru.ksu.niimm.cll.anduin.description

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import ru.ksu.niimm.cll.anduin.util.PredicateGroupCodes._
import com.twitter.scalding.{Tsv, JobTest, TypedTsv}
import ru.ksu.niimm.cll.anduin.util.NodeParser._

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class MergeEntityDescriptionProcessorTest extends JUnit4(MergeEntityDescriptionProcessorTestSpec)

object MergeEntityDescriptionProcessorTestSpec extends Specification {
  "The merge entity description processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.description.MergeEntityDescriptionProcessor").
      arg("entityAttributes", "entityAttributesFile").
      arg("similarEntityNames", "similarEntityNamesFile").
      arg("output", "outputFile")
      .source(TypedTsv[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)]("similarEntityNamesFile"), List(
      (SIMILAR_ENTITY_NAMES, "<http://dbpedia.org/resource/Author>", "\"Author 2\"@en"),
      (SIMILAR_ENTITY_NAMES, "<http://dbpedia.org/resource/Category:American_physicists>", "American physicists"),
      (SIMILAR_ENTITY_NAMES, "<http://dbpedia.org/resource/Scientist>", "Scientist")
    ))
      .source(TypedTsv[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)]("entityAttributesFile"), List(
      (NAMES, "<http://dbpedia.org/resource/Author>", "\"Author 1\"@en"),
      (OUTGOING_ENTITY_NAMES, "<http://dbpedia.org/resource/Category:American_physicists>", "type")
    )).
      sink[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct entity descriptions" in {
          outputBuffer.size must_== 5
          outputBuffer(0) mustEqual(NAMES, "<http://dbpedia.org/resource/Author>", "\"Author 1\"@en")
          outputBuffer(1) mustEqual(SIMILAR_ENTITY_NAMES, "<http://dbpedia.org/resource/Author>", "\"Author 2\"@en")
          outputBuffer(2) mustEqual(SIMILAR_ENTITY_NAMES, "<http://dbpedia.org/resource/Category:American_physicists>", "American physicists")
          outputBuffer(3) mustEqual(OUTGOING_ENTITY_NAMES, "<http://dbpedia.org/resource/Category:American_physicists>", "type")
          outputBuffer(4) mustEqual(SIMILAR_ENTITY_NAMES, "<http://dbpedia.org/resource/Scientist>", "Scientist")
        }
    }.run.
      finish
  }
}
