package ru.ksu.niimm.cll.anduin.adjacency

import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import scala.Range
import com.twitter.scalding.Tsv
import com.twitter.scalding.TextLine

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
    TypedTsv[(String, String)](args("inputPredicateList")).read.rename((0, 1) ->('relPredicate, 'relPredicateId))
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
  }.unique(('subject, 'predicate, 'object))
    .joinWithTiny('predicate -> 'relPredicate, relevantPredicates)
    .project(('relPredicateId, 'subject, 'object))

  triples
    .write(Tsv(args("output")))
}
