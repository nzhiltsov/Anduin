package ru.ksu.niimm.cll.anduin

import com.twitter.scalding._
import com.twitter.scalding.TextLine
import ru.ksu.niimm.cll.anduin.NodeParser._
import cascading.pipe.Pipe
import cascading.pipe.joiner.LeftJoin

/**
 * @author Nikita Zhiltsov 
 */
class DocumentEntityProcessor(args: Args) extends Job(args) {

  val lines = TextLine(args("input")).read

  def process(pipe: Pipe) = pipe.map('line ->('context, 'subject, 'predicate, 'object)) {
    line: String => extractNodes(line)
  }.discard(('line, 'num))


  val firstLevelEntities = process(lines)

  val secondLevelEntities =
    firstLevelEntities.rename(('context, 'subject, 'predicate, 'object) ->
      ('context2, 'subject2, 'predicate2, 'object2)).filter('subject2) {
      subject: Subject => subject.startsWith("<")
    }

  val secondLevelBNodes = firstLevelEntities.rename(('context, 'subject, 'predicate, 'object) ->
    ('context3, 'subject3, 'predicate3, 'object3)).filter('subject3) {
    subject: Subject => subject.startsWith("_")
  }

  val mergedEntities = firstLevelEntities.filter('subject) {
    subject: Subject => subject.startsWith("<")
  }
    .joinWithSmaller(('object -> 'subject2), secondLevelEntities, joiner = new LeftJoin)
    .joinWithSmaller(('context, 'object) ->('context3, 'subject3), secondLevelBNodes, joiner = new LeftJoin)
    .project(('subject, 'predicate, 'object, 'object2, 'object3))
    .map(('object, 'object2, 'object3) -> ('objects)) {
    fields: (Range, Range, Range) =>
      if (fields._2 != null) {
        fields._2
      } else if (fields._3 != null) {
        fields._3
      } else {
        fields._1
      }
  }.project(('subject, 'predicate, 'objects))

  mergedEntities.
    groupBy(('subject, 'predicate)) {
    _.reduce('objects -> 'objects) {
      (a: Range, b: Range) => a + " " + b
    }
  }.map('objects -> 'objects) {
    range: Range => range + " ."
  }
    .write(Tsv(args("output")))
}