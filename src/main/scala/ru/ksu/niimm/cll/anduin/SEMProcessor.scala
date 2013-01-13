package ru.ksu.niimm.cll.anduin

import com.twitter.scalding.{TextLine, Job, Args}
import util.{FixedPathLzoTsv, NodeParser}
import NodeParser._
import cascading.pipe.joiner.{LeftJoin, InnerJoin}

/**
 * This processor implements aggregation of entity description with partial URI resolution according to the paper
 * Neumayer, R., Balog, K. On the Modeling of Entities for Ad-Hoc Entity Search in the Web of Data. ECIR'12
 * <br/>
 * The output format is as follows:
 * <p>
 * <b>predicate_type TAB subject TAB objects</b>
 * </p>
 * where 'predicate_type' values are {0,1,2,3}, i.e., {nameType, attrType, outRelType, inRelType}, correspondingly,
 * and 'objects' may contain more than one literal, the literals are delimited with space, e.g.
 * <p>
 * <b>2	<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>	<http://www.aktors.org/ontology/portal#has-author>	"Tyson" "Johnson"</b>
 * </p>
 *
 * All the unresolved URIs are normalized to simplify further tokenization.
 *
@author Nikita Zhiltsov
 */
class SEMProcessor(args: Args) extends Job(args) {
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
  private val lines = new TextLine(args("input")).read.filter('line) {
    line: String =>
      line.length < maxLineLength
  }
  /**
   * extracts the unique quad nodes from lines
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
  }.project(('subject, 'predicate, 'object)).unique(('subject, 'predicate, 'object))
  /**
   * filters first level entities with blank nodes as object for resolution
   */
  private val firstLevelEntitiesWithBNodesAsObjects = firstLevelEntitiesWithoutBNodes.filter('object) {
    range: Range => range.startsWith("_")
  }.unique(('context, 'subject, 'predicate, 'object))
  /**
   * filters first level entities with literal values
   */
  private val firstLevelEntitiesWithLiterals = firstLevelEntitiesWithoutBNodes.filter('object) {
    range: Range => range.startsWith("\"")
  }.project(('subject, 'predicate, 'object)).unique(('subject, 'predicate, 'object)).groupBy(('subject, 'predicate)) {
    _.mkString('object, " ")
  }.map('predicate -> 'predicatetype) {
    predicate: Predicate => if (isNamePredicate(predicate)) 0 else 1
  }.unique(('predicatetype, 'subject, 'object))
    .project(('predicatetype, 'subject, 'object))
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
  }.unique(('predicatetype, 'subject, 'object))
    .project(('predicatetype, 'subject, 'object))
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

  val URL_ENCODING_ELEMENT_PATTERN: String = "%2[0-9]{1}"
  val SPECIAL_SYMBOL_PATTERN: String = "[\\._:'/<>]"

  /**
   * replaces all the special symbols with spaces in a given entity URI
   *
   * @param str  entity URI
   * @return
   */
  def stripURI(str: String) = str.replaceAll(URL_ENCODING_ELEMENT_PATTERN, " ").replaceAll(SPECIAL_SYMBOL_PATTERN, " ")

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
  private val mergedEntities =
    firstLevelEntitiesWithLiterals ++ entitiesWithResolvedBNodes ++ entitiesWithResolvedURIs ++ entitiesWithUnresolvedURIs ++ incomingLinks ++ unresolvedIncomingLinks

  mergedEntities
    .write(new FixedPathLzoTsv(args("output")))
}
