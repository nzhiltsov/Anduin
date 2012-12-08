package ru.ksu.niimm.cll.anduin

import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.NodeParser._
import cascading.pipe.joiner.LeftJoin

/**
 * This processor implements aggregation of entity descriptions with full URI resolution
 *
 * @author Nikita Zhiltsov 
 */
class DocumentEntityProcessor(args: Args) extends AbstractProcessor(args) {
  private val secondLevelEntities =
    firstLevelEntities.rename(('context, 'subject, 'predicate, 'object) ->
      ('context2, 'subject2, 'predicate2, 'object2)).filter(('subject2, 'object2)) {
      fields: (Subject, Range) => fields._1.startsWith("<") && fields._2.startsWith("\"")
    }

  private val secondLevelBNodes = firstLevelEntities.rename(('context, 'subject, 'predicate, 'object) ->
    ('context3, 'subject3, 'predicate3, 'object3)).filter(('subject3, 'object3)) {
    fields: (Subject, Range) => fields._1.startsWith("_") && fields._2.startsWith("\"")
  }

  private val mergedEntities = firstLevelEntitiesWithoutBNodes
    .joinWithSmaller(('context, 'object) ->('context3, 'subject3), secondLevelBNodes, joiner = new LeftJoin)
    .joinWithSmaller(('object -> 'subject2), secondLevelEntities, joiner = new LeftJoin)
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
    .groupBy(('subject, 'predicate)) {
    _.reduce('objects -> 'objects) {
      (a: Range, b: Range) => a + " " + b
    }
  }

  mergedEntities.groupAll {
    _.sortBy('subject)
  }
    .write(Tsv(args("output")))
}