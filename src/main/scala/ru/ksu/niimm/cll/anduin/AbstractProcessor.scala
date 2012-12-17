package ru.ksu.niimm.cll.anduin

import com.twitter.scalding.{Job, Args}
import cascading.pipe.Pipe
import ru.ksu.niimm.cll.anduin.NodeParser._
import com.twitter.scalding.TextLine

/**
 * @author Nikita Zhiltsov 
 */
class AbstractProcessor(args: Args) extends Job(args) {
  private val lines = TextLine(args("input")).read

  protected def process(pipe: Pipe, offsetName: Symbol) = pipe.mapTo('line ->('context, 'subject, 'predicate, 'object)) {
    line: String => extractNodes(line)
  }

  protected val firstLevelEntities = process(lines, 'offset)

  protected val firstLevelEntitiesWithoutBNodes = firstLevelEntities.filter('subject) {
    subject: Subject => subject.startsWith("<")
  }
}
