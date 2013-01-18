package ru.ksu.niimm.cll.anduin

import com.twitter.scalding._
import util.{FixedPathLzoTextLine, NodeParser}
import NodeParser._

/**
 * This processor outputs adjacency lists filtered and grouped by given predicates in the form of:
 * The output format is as follows:
 * <p>
 * <b>predicate_id TAB subject TAB object</b>
 * </p>
 * where predicate_id is an integer identifier of a predicate, and subject and object are entity URIs.
 * The output is sorted by predicate_id and subject values
 *
 * @author Nikita Zhiltsov 
 */
class AdjacencyListProcessor(args: Args) extends Job(args) {
  /**
   * reads the predicates of interest
   */
  private val relevantPredicates =
    TypedTsv[(String, Int)](args("inputPredicateList")).read.rename((0, 1) ->('relPredicate, 'relPredicateId))
  /**
   * reads the relevant entities to filter the edges
   */
  private val candidateEntities =
    new TextLine(args("inputCandidateList")).read.map('line -> 'line) {
      line: String => line.mkString("<", "", ">")
    }
  /**
   * reads the entity triples
   */
  private val triples = TextLine(args("input")).read.mapTo('line ->('subject, 'predicate, 'object)) {
    line: String =>
      val nodes = extractNodes(line)
      (nodes._2, nodes._3, nodes._4)
  }.filter(('subject, 'object)) {
    fields: (Subject, Range) =>
      fields._1.startsWith("<") && fields._2.startsWith("<")
  }.joinWithTiny('predicate -> 'relPredicate, relevantPredicates)
    .project(('relPredicateId, 'subject, 'object))
    .unique(('relPredicateId, 'subject, 'object))


  private val filteredTriples =
    triples.joinWithSmaller('subject -> 'line, candidateEntities).project(('relPredicateId, 'subject, 'object))
      .joinWithSmaller('object -> 'line, candidateEntities).project(('relPredicateId, 'subject, 'object))

  filteredTriples
    .write(Tsv(args("output")))
}
