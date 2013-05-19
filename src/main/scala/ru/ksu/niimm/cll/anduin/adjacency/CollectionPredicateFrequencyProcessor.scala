package ru.ksu.niimm.cll.anduin.adjacency

import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import com.twitter.scalding.TextLine

/**
 * This processor outputs unique predicates (object properties) with their frequencies for the whole collection
 *
 * @author Nikita Zhiltsov 
 */
class CollectionPredicateFrequencyProcessor(args: Args) extends Job(args) {
  /**
   * reads the triples
   */
  private val triples = TextLine(args("input")).read.mapTo('line ->('subject, 'predicate, 'object)) {
    line: String =>
      val nodes = extractNodes(line)
      (nodes._2, nodes._3, nodes._4)
  }.filter(('subject, 'object)) {
    fields: (Subject, Range) =>
      fields._1.startsWith("<") && fields._2.startsWith("<")
  }.unique(('subject, 'predicate, 'object))

  triples.groupBy('predicate) {
    _.size('count)
  }.groupAll {
    _.sortBy('count).reverse
  }.project(('predicate, 'count)).write(Tsv(args("output")))
}
