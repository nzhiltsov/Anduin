package ru.ksu.niimm.cll.anduin.sem

import com.twitter.scalding.{Tsv, TypedTsv, Job, Args}
import cascading.pipe.joiner.{LeftJoin, InnerJoin}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTsv

/**
 * This processor resolves outgoing links in entity descriptions
 *
 * @author Nikita Zhiltsov 
 */
class OutgoingLinkProcessor(args: Args) extends Job(args) {
  private val firstLevelEntities =
    TypedTsv[(Context, Subject, Predicate, Range)](args("input")).read.rename((0, 1, 2, 3) ->('context, 'subject, 'predicate, 'object))
  /**
   * filters first level blank nodes
   */
  private val firstLevelBNodes = firstLevelEntities.filter('subject) {
    subject: Subject => subject.startsWith("_")
  }

  /**
   * filters first level entities with URIs as subjects, i.e. candidates for further indexing
   */
  private val firstLevelEntitiesWithoutBNodes = firstLevelEntities.filter('subject) {
    subject: Subject => subject.startsWith("<")
  }
  /**
   * filters second level blank nodes with 'name'-like predicates and literal values; merges the literal values
   */
  private val secondLevelBNodes = firstLevelBNodes.rename(('context, 'subject, 'predicate, 'object) ->
    ('context3, 'subject3, 'predicate3, 'object3)).filter(('predicate3, 'object3)) {
    fields: (Predicate, Range) => isNamePredicate(fields._1) && fields._2.startsWith("\"")
  }.groupBy(('context3, 'subject3, 'predicate3)) {
    _.mkString('object3, " ")
  }
  /**
   * filters first level entities with blank nodes as object for resolution
   */
  private val firstLevelEntitiesWithBNodesAsObjects = firstLevelEntitiesWithoutBNodes.filter('object) {
    range: Range => range.startsWith("_")
  }.unique(('context, 'subject, 'predicate, 'object))
  /**
   * resolves blank nodes; matching of two blank nodes makes sense, only if both the contexts are the same
   */
  private val entitiesWithResolvedBNodes = firstLevelEntitiesWithBNodesAsObjects
    .joinWithSmaller(('context, 'object) ->('context3, 'subject3), secondLevelBNodes, joiner = new InnerJoin)
    .project(('subject, 'predicate, 'object3)).rename('object3 -> 'object).map('predicate -> 'predicatetype) {
    predicate: Predicate => 2
  }.unique(('predicatetype, 'subject, 'object))
    .project(('predicatetype, 'subject, 'object))
    .map('object -> 'object) {
    range: Range =>
      cleanHTMLMarkup(range)
  }

  /**
   * filters first level entities with URIs as objects for resolution
   */
  private val firstLevelEntitiesWithURIsAsObjects = firstLevelEntitiesWithoutBNodes.filter('object) {
    range: Range => range.startsWith("<")
  }.project(('subject, 'predicate, 'object)).unique(('subject, 'predicate, 'object))

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
   * resolves URIs as objects across the whole data set
   */
  private val entitiesWithResolvedURIs = firstLevelEntitiesWithURIsAsObjects
    .joinWithSmaller(('object -> 'subject2), secondLevelEntities, joiner = new InnerJoin)
    .project(('subject, 'predicate, 'object2))
    .rename('object2 -> 'object)
    .map('predicate -> 'predicatetype) {
    predicate: Predicate => 2
  }.unique(('predicatetype, 'subject, 'object))
    .project(('predicatetype, 'subject, 'object))
    .map('object -> 'object) {
    range: Range =>
      cleanHTMLMarkup(range)
  }

  /**
   * entities with unresolved URIs
   */
  private val entitiesWithUnresolvedURIs = firstLevelEntitiesWithURIsAsObjects
    .joinWithSmaller(('object -> 'subject2), secondLevelEntities, joiner = new LeftJoin)
    .filter('object2) {
    range: Range =>
      range == null
  }.project(('subject, 'predicate, 'object))
    .map('object -> 'object) {
    range: Range =>
      stripURI(range)
  }
    .map('predicate -> 'predicatetype) {
    predicate: Predicate => 2
  }.unique(('predicatetype, 'subject, 'object))
    .project(('predicatetype, 'subject, 'object))
    .map('object -> 'object) {
    range: Range =>
      cleanHTMLMarkup(range)
  }

  /**
   * combines all the pipes into the single final pipe
   */
  private val mergedEntities =
    entitiesWithResolvedBNodes ++ entitiesWithResolvedURIs ++ entitiesWithUnresolvedURIs

  /**
   * cleans and outputs the data
   */
  mergedEntities
    .unique(('predicatetype, 'subject, 'object))
    .write(Tsv(args("output")))
}
