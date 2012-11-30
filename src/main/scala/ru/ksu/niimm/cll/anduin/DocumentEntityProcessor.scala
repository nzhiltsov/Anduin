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
  }.discard(('line, 'num)).
    groupBy(('context, 'subject, 'predicate)) {
    _.sortBy(('context, 'subject, 'predicate))
  }


  val firstLevelEntities = process(lines)

  val secondLevelEntities =
    firstLevelEntities.rename(('context, 'subject, 'predicate, 'object) ->
      ('context2, 'subject2, 'predicate2, 'object2))

  //  firstLevelEntities.groupBy(('subject, 'predicate)) {
  //    _.reduce('object -> 'object) {
  //      (a: Range, b: Range) => a + " " + b
  //    }
  //  }.
  val mergedEntities = firstLevelEntities.joinWithSmaller((('context, 'object) ->('context2, 'subject2)), secondLevelEntities, joiner = new LeftJoin)
    .project(('context, 'subject, 'predicate, 'object, 'object2))
    .map(('object, 'object2) -> ('objects)) {
    fields: (Range, Range) => if (fields._2 != null) {
      fields._2
    } else {
      fields._1
    }
  }.project(('context, 'subject, 'predicate, 'objects))

  mergedEntities.
    groupBy(('context, 'subject, 'predicate)) {
    _.reduce('objects -> 'objects) {
      (a: Range, b: Range) => a + " " + b
    }
  }.discard('context).map('objects -> 'objects) {
    range: Range => range + " ."
  }
    .write(Tsv(args("output")))
}