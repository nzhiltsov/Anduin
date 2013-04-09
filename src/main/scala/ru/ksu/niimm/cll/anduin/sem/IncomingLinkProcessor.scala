package ru.ksu.niimm.cll.anduin.sem

import com.twitter.scalding.{Tsv, TypedTsv, Job, Args}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import cascading.pipe.joiner.InnerJoin

/**
 * This processor resolves incoming links in entity descriptions
 *
 * @author Nikita Zhiltsov 
 */
class IncomingLinkProcessor(args: Args) extends Job(args) {
  private val firstLevelEntities =
    TypedTsv[(Subject, Predicate, Range)](args("inputFirstLevel")).read.rename((0, 1, 2) ->('subject, 'predicate, 'object))
      .project(('subject, 'object))
      .filter(('subject, 'object)) {
      fields: (Subject, Range) => fields._1.startsWith("<") && fields._2.startsWith("<")
    }
  /**
   * filters second level entities with URIs as objects, 'name'-like predicates and literal values;
   * merges the literal values
   */
  private val secondLevelEntities =
    TypedTsv[(String, Subject, Range)](args("inputSecondLevel")).read.rename((0, 1, 2) ->('predicatetype, 'subject2, 'object2))
      .filter('predicatetype) {
      predicateType: String => predicateType.equals("0")
    }
      .project(('subject2, 'object2))

  firstLevelEntities
    .joinWithSmaller(('subject -> 'subject2), secondLevelEntities, joiner = new InnerJoin)
    .project(('object, 'object2))
    .rename(('object, 'object2) ->('subject, 'object))
    .map('subject -> 'predicatetype) {
    predicate: Predicate => 3
  }
    .project(('predicatetype, 'subject, 'object))
    .write(Tsv(args("output")))
}
