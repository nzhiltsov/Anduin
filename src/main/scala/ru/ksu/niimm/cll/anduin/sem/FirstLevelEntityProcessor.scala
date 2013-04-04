package ru.ksu.niimm.cll.anduin.sem

import com.twitter.scalding.{Tsv, Job, Args, TextLine}
import ru.ksu.niimm.cll.anduin.util.NodeParser._

/**
 * This processor output the entity descriptions
 *
 * @author Nikita Zhiltsov 
 */
class FirstLevelEntityProcessor(args: Args) extends Job(args) {
  val MAX_LINE_LENGTH = 100000
  /**
   * filters out too long lines and reads the context-subject-predicate-object quads
   */
  private val quads = new TextLine(args("input")).read
    .filter('line) {
    line: String =>
      line.length < MAX_LINE_LENGTH
  }
    .mapTo('line ->('context, 'subject, 'predicate, 'object)) {
    line: String => extractNodes(line)
  }
  /**
   * filters out non-English literals
  */
  quads
    .filter('object) {
      range: Range => !range.contains("\"@") || range.contains("\"@en")
  }
    .write(Tsv(args("output")))
}
