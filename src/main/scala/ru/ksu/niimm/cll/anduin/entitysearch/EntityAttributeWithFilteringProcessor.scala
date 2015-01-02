package ru.ksu.niimm.cll.anduin.entitysearch

import com.twitter.scalding.{Job, Args, TypedTsv, TextLine, Tsv}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.NodeParser.Range
import ru.ksu.niimm.cll.anduin.util.PredicateGroupCodes._
import cascading.pipe.joiner.LeftJoin

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
    .filter('object) {
    range: Range => if (range.startsWith("\"") && !range.endsWith("\"")) range.endsWith("@en") else true
  }

  val allAttributes = filterGraph.joinWithSmaller('object -> 'entityUri, entityNames, joiner = new LeftJoin)
    .project(('subject, 'predicate, 'object, 'names))
    .rename('names -> 'objectName)
    // ('subject, 'predicate, 'object, 'objectName)
    .joinWithSmaller('predicate -> 'entityUri, entityNames, joiner = new LeftJoin)
    .project(('subject, 'predicate, 'object, 'objectName, 'names))
    .rename('names -> 'predicatename)
    .mapTo(('subject, 'predicate, 'object, 'objectName, 'predicatename) ->('predicatetype, 'subject, 'object)) {
    fields: (Subject, Predicate, Range, String, String) =>
      val predicateType = encodePredicateType(fields._2, !fields._3.startsWith("<"))
      val content = predicateType match {
        case CATEGORIES => if (fields._4 != null) fields._4 else ""
        case NAMES => fields._3
        case ATTRIBUTES =>
          if (fields._5 != null && fields._3 != null) fields._5 + " " + fields._3 else if (fields._3 != null) fields._3 else ""
        case OUTGOING_ENTITY_NAMES =>
          if (fields._5 != null && fields._4 != null) fields._5 + " " + fields._4 else if (fields._4 != null) fields._4 else ""
      }
      (predicateType, fields._1, content.trim)
  }
  .filter('object) {
    range: Range => range.length != 0
  }

  allAttributes
    .groupBy(('subject, 'predicatetype)) {
    _.mkString('object, " ")
  }
    .map('object -> 'object) {
    range: Range =>
      cleanHTMLMarkup(range)
  }
    .groupBy(('subject, 'predicatetype)) {
    _.reducers(10).sortBy(('subject, 'predicatetype))
  }
    .project(('predicatetype, 'subject, 'object))
    .write(Tsv(args("output")))
}
