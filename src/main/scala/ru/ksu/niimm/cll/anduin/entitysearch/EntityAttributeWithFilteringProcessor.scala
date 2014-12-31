package ru.ksu.niimm.cll.anduin.entitysearch

import com.twitter.scalding.{Job, Args, TypedTsv, TextLine, Tsv}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.NodeParser.Range
import ru.ksu.niimm.cll.anduin.util.PredicateGroupCodes._

/**
 * Given an input of quads,
 * this processor outputs the values of entity attributes distinguishing attributes according to different groups
 * @see EntityAttributeProcessor
 * @author Nikita Zhiltsov
 */
class EntityAttributeWithFilteringProcessor(args: Args) extends Job(args) {
  /**
   * The predicates that should be excluded from the output here
   */
  val blackListedPredicates = List(OWL_SAMEAS_PREDICATE, DBPEDIA_DISAMBIGUATES_PREDICATE, DBPEDIA_REDIRECT_PREDICATE,
    DBPEDIA_WIKI_PAGE_WIKI_LINK, DBPEDIA_WIKIPAGE_EXTERNAL_LINK)

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
  }.filter('predicate) {
    predicate: Predicate => !blackListedPredicates.contains(predicate)
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
  }.project(('predicatetype, 'subject, 'object))
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
  }.project(('predicatetype, 'subject, 'object))
  /**
   * extracts the names of adjacent predicates
   */
  private val adjacentPredicateNames = firstLevelEntitiesWithoutBNodes
    .joinWithSmaller('predicate -> 'entityUri, entityNames).project(('subject, 'names, 'object))
    .rename('names -> 'predicatename)
    .joinWithSmaller('object -> 'entityUri, entityNames)
    .project(('subject, 'predicatename, 'names))
    .rename('names -> 'values)
    .mapTo(('subject, 'predicatename, 'values) ->('subject, 'predicatetype, 'object)) {
    fields: (Subject, String, String) =>
      (fields._1, PREDICATE_NAMES, fields._2 + " " + fields._3)
  }.project(('predicatetype, 'subject, 'object))

  private val allAttributes = firstLevelEntitiesWithDatatypeProperties ++ resolvedObjectNames ++ adjacentPredicateNames

  allAttributes
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
