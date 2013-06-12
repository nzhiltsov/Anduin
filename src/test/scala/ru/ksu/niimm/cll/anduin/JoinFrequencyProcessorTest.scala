package ru.ksu.niimm.cll.anduin

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding._

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class JoinFrequencyProcessorTest extends JUnit4(JoinFrequencyProcessorTestSpec)

object JoinFrequencyProcessorTestSpec extends Specification with TupleConversions {
  "Join frequency processor" should {
    JobTest("ru.ksu.niimm.cll.anduin.JoinFrequencyProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(TypedTsv[(String, Int)]("inputFile"), List(
      // 1st row
      ("abc", 4),
      // 2nd row
      ("cde", 10),
      // 3rd row
      ("abc", 50),
      // 4th row
      ("cde", 2)
    )).
      sink[(String, Int)](Tsv("outputFile")) {
      outputBuffer =>
        "output correct word counts" in {
          outputBuffer.size must_== 2
          outputBuffer(0)._1 mustEqual "abc"
          outputBuffer(0)._2 must_== 54
          outputBuffer(1)._1 mustEqual "cde"
          outputBuffer(1)._2 must_== 12
        }
    }.run.
      finish
  }
}
