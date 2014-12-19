package ru.ksu.niimm.cll.anduin.entitysearch

import com.twitter.scalding.{Job, Args, TypedTsv, TextLine, Tsv}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.NodeParser.Range
import ru.ksu.niimm.cll.anduin.util.PredicateGroupCodes._

/**
 * Given an input of quads,
 * this processor outputs the values of entity attributes distinguishing 'name'-like attributes
 * @see EntityAttributeProcessor
 * @author Nikita Zhiltsov
 */
class EntityAttributeWithFilteringProcessor(args: Args) extends Job(args) {
  private val inputFormat = args("inputFormat")

  def isNquad = inputFormat.equals("nquad")

  private val relevantPredicates =
    TypedTsv[(String, String)](args("inputPredicates")).read.rename((0, 1) ->('relPredicateId, 'relPredicate))

  private val entityNames =
    TypedTsv[(String, String)](args("entityNames")).read.rename((0, 1) ->('entityUri, 'names))

  private val firstLevelEntitiesWithoutBNodes = TextLine(args("input")).read
    .filter('line) {
    line: String =>
      val cleanLine = line.trim
      cleanLine.startsWith("<")
  }
    .mapTo('line ->('subject, 'predicate, 'object)) {
    line: String =>
      if (isNquad) {
        val nodes = extractNodes(line)
        (nodes._2, nodes._3, nodes._4)
      } else extractNodesFromN3(line)
  }

  /**
   * retains only relevant predicates
   */
  private val filterGraph = firstLevelEntitiesWithoutBNodes
    .joinWithTiny('predicate -> 'relPredicate, relevantPredicates).project(('subject, 'predicate, 'object))

  private val firstLevelEntititesWithObjectLinks = filterGraph.filter('object) {
    range: Range => range.startsWith("<")
  }
  /**
   * objects with resolved names
   */
  private val resolvedObjectNames = firstLevelEntititesWithObjectLinks
    .joinWithSmaller('object -> 'entityUri, entityNames).project(('subject, 'predicate, 'names))
   .rename('names -> 'object)
    .map('predicate -> 'predicatetype) {
    predicate: Predicate => encodePredicateType(predicate, false)
  }
  /**
   * filters first level entities with literal values
   */
  private val firstLevelEntitiesWithDatatypeProperties = filterGraph.filter('object) {
    range: Range => !range.startsWith("<")
  }
    .filter('object) {
    range: Range => if (range.startsWith("\"") && !range.endsWith("\"")) range.endsWith("@en") else true
  }.map('predicate -> 'predicatetype) {
    predicate: Predicate => encodePredicateType(predicate, true)
  }
  /**
   * extracts the names of adjacent predicates
   */
  private val adjacentPredicateNames = firstLevelEntitiesWithoutBNodes.project(('subject, 'predicate)).unique(('subject, 'predicate))
  .joinWithSmaller('predicate -> 'entityUri, entityNames).project(('subject, 'predicate, 'names))
    .rename('names -> 'object)
    .map('predicate -> 'predicatetype) {
    predicate: Predicate => PREDICATE_NAMES
  }

  private val allAttributes = firstLevelEntitiesWithDatatypeProperties ++ resolvedObjectNames ++ adjacentPredicateNames

  allAttributes
    .project(('predicatetype, 'subject, 'object))
    .groupBy(('subject, 'predicatetype)) {
    _.mkString('object, " ")
  }
    .project(('predicatetype, 'subject, 'object))
    .map('object -> 'object) {
    range: Range =>
      cleanHTMLMarkup(range)
  }
    .groupBy(('subject, 'predicatetype)) {
    _.reducers(10).sortBy(('subject, 'predicatetype))
  }
    .write(Tsv(args("output")))
}
