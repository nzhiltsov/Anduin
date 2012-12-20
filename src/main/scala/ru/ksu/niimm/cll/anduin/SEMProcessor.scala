package ru.ksu.niimm.cll.anduin

import com.twitter.scalding.{Job, TextLine, Tsv, Args}
import util.NodeParser
import NodeParser._
import cascading.pipe.joiner.InnerJoin

/**
 * This processor implements aggregation of entity description with partial URI resolution according to the paper
 * Neumayer, R., Balog, K. On the Modeling of Entities for Ad-Hoc Entity Search in the Web of Data. ECIR'12
 * <br/>
 * The output format is as follows:
 * <p>
 * <b>predicate_type TAB subject TAB predicate TAB objects</b>
 * </p>
 * where 'predicate_type' values are {0,1,2}, i.e. {nameType, attrType, outRelType}, correspondingly,
 * and 'objects' may contain more than one literals delimited with space, e.g.
 * <p>
 * <b>2	<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>	<http://www.aktors.org/ontology/portal#has-author>	"Tyson" "Johnson"</b>
 * </p>
 @author Nikita Zhiltsov
 */
class SEMProcessor(args: Args) extends Job(args) {
  private val wordDelimiterRegex = "[^a-zA-Z]"
  private val maxLineLength = 40000

  private val namePattern = "^<http.*(label|name|title)>$"

  /**
   * check if the predicate is 'name'-like, e.g. 'name', 'label', 'title' etc.
   *
   * @param predicate a predicate
   * @return
   */
  def isNamePredicate(predicate: Predicate): Boolean =
    predicate.matches(namePattern)

  /**
   * reads raw lines and filters out too large ones
   */
  private val lines = TextLine(args("input")).read.filter('line) {
    line: String =>
      line.length < maxLineLength
  }
  /**
   * extracts the quad nodes from lines
   */
  private val firstLevelEntities = lines.mapTo('line ->('context, 'subject, 'predicate, 'object)) {
    line: String => extractNodes(line)
  }
  /**
   * filters first level entities with URIs as subjects, i.e. candidates for further indexing
   */
  private val firstLevelEntitiesWithoutBNodes = firstLevelEntities.filter('subject) {
    subject: Subject => subject.startsWith("<")
  }
  /**
   * filters first level entities with URIs as objects for resolution
   */
  private val firstLevelEntitiesWithURIsAsObjects = firstLevelEntitiesWithoutBNodes.filter('object) {
    range: Range => range.startsWith("<")
  }.project(('subject, 'predicate, 'object))
  /**
   * filters first level entities with blank nodes as object for resolution
   */
  private val firstLevelEntitiesWithBNodesAsObjects = firstLevelEntitiesWithoutBNodes.filter('object) {
    range: Range => range.startsWith("_")
  }
  /**
   * filters first level entities with literal values
   */
  private val firstLevelEntitiesWithLiterals = firstLevelEntitiesWithoutBNodes.filter('object) {
    range: Range => range.startsWith("\"")
  }.project(('subject, 'predicate, 'object)).unique(('subject, 'predicate, 'object)).groupBy(('subject, 'predicate)) {
    _.mkString('object, " ")
  }.map('predicate -> 'predicatetype) {
    predicate: Predicate => if (isNamePredicate(predicate)) 0 else 1
  }
  /**
   * filters first level blank nodes
   */
  private val firstLevelBNodes = firstLevelEntities.filter('subject) {
    subject: Subject => subject.startsWith("_")
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
   * filters second level blank nodes with 'name'-like predicates and literal values; merges the literal values
   */
  private val secondLevelBNodes = firstLevelBNodes.rename(('context, 'subject, 'predicate, 'object) ->
    ('context3, 'subject3, 'predicate3, 'object3)).filter(('predicate3, 'object3)) {
    fields: (Predicate, Range) => isNamePredicate(fields._1) && fields._2.startsWith("\"")
  }.groupBy(('context3, 'subject3, 'predicate3)) {
    _.mkString('object3, " ")
  }
  /**
   * resolves blank nodes; matching of two blank nodes makes sense, only if both the contexts are the same
   */
  private val entitiesWithResolvedBNodes = firstLevelEntitiesWithBNodesAsObjects
    .joinWithSmaller(('context, 'object) ->('context3, 'subject3), secondLevelBNodes, joiner = new InnerJoin)
    .project(('subject, 'predicate, 'object3)).rename('object3 -> 'object).map('predicate -> 'predicatetype) {
    predicate: Predicate => 2
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
  }
  /**
   * combines all the pipes into the single final pipe
   */
  private val mergedEntities = firstLevelEntitiesWithLiterals ++ entitiesWithResolvedBNodes ++ entitiesWithResolvedURIs

  mergedEntities.groupBy(('subject, 'predicate, 'predicatetype)) {
    _.mkString('object, " ")
  }.project(('predicatetype, 'subject, 'predicate, 'object))
    .groupAll {
    _.sortBy('subject)
  }.write(Tsv(args("output")))
}
