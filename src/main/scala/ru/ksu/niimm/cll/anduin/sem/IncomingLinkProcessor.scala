package ru.ksu.niimm.cll.anduin.sem

import com.twitter.scalding.{Tsv, TypedTsv, Job, Args}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import cascading.pipe.joiner.LeftJoin

/**
 * This processor resolves incoming links in entity descriptions
 *
 * @author Nikita Zhiltsov 
 */
class IncomingLinkProcessor(args: Args) extends Job(args) {
  private val firstLevelEntities =
    TypedTsv[(Context, Subject, Predicate, Range)](args("input")).read.rename((0, 1, 2, 3) ->('context, 'subject, 'predicate, 'object))
  /**
   * filters first level entities with URIs as subjects, i.e. candidates for further indexing
   */
  private val firstLevelEntitiesWithoutBNodes = firstLevelEntities.filter('subject) {
    subject: Subject => subject.startsWith("<")
  }
  /**
   * filters second level entities with URIs as objects, 'name'-like predicates and literal values;
   * merges the literal values
   */
  private val secondLevelEntities =
    firstLevelEntitiesWithoutBNodes.rename(('context, 'subject, 'predicate, 'object) ->
      ('context2, 'subject2, 'predicate2, 'object2)).project(('subject2, 'predicate2, 'object2))
      .filter(('predicate2, 'object2)) {
      fields: (Predicate, Range) => isNamePredicate(fields._1) && fields._2.startsWith("\"")
    }
      .unique(('subject2, 'predicate2, 'object2))
      .groupBy(('subject2, 'predicate2)) {
      _.mkString('object2, " ")
    }
  /**
   * filters first level entities with URIs as objects for resolution
   */
  private val firstLevelEntitiesWithURIsAsObjects = firstLevelEntitiesWithoutBNodes.filter('object) {
    range: Range => range.startsWith("<")
  }.project(('subject, 'predicate, 'object)).unique(('subject, 'predicate, 'object))
  /**
   * entities with resolved incoming links
   */
  private val incomingLinks =
    firstLevelEntitiesWithURIsAsObjects.joinWithSmaller(('subject -> 'subject2), secondLevelEntities)
      .project(('object, 'object2)).rename(('object, 'object2) ->('subject, 'object))
      .unique(('subject, 'object))
      .map('subject -> 'predicatetype) {
      subject: Subject => 3
    }
      .project(('predicatetype, 'subject, 'object))

  /**
   * entities with unresolved incoming links, the unresolved URIs will be normalized
   */
  private val unresolvedIncomingLinks =
    firstLevelEntitiesWithURIsAsObjects.joinWithSmaller(('subject -> 'subject2), secondLevelEntities, joiner = new LeftJoin)
      .filter('object2) {
      range: Range =>
        range == null
    }.project(('object, 'subject)).rename(('object, 'subject) ->('subject, 'object))
      .unique(('subject, 'object))
      .map('object -> 'object) {
      range: Range =>
        stripURI(range)
    }
      .map('subject -> 'predicatetype) {
      subject: Subject => 3
    }
      .project(('predicatetype, 'subject, 'object))
  /**
   * combines all the pipes into the single final pipe
   */
  private val mergedEntities = incomingLinks ++ unresolvedIncomingLinks

  /**
   * cleans and outputs the data
   */
  mergedEntities
    .unique(('predicatetype, 'subject, 'object))
    .map('object -> 'object) {
    range: Range =>
      cleanHTMLMarkup(range)
  }
    .write(Tsv(args("output")))
}
