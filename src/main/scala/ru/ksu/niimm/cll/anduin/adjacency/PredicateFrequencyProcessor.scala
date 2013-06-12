package ru.ksu.niimm.cll.anduin.adjacency

import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import com.twitter.scalding.TextLine
import scala.io.Source

/**
 * Given two lists of entities,
 * this processor outputs frequencies of predicates as edges in the bipartite graph
 *
 * @author Nikita Zhiltsov 
 */
class PredicateFrequencyProcessor(args: Args) extends Job(args) {
  private val firstEntities =
    Source.fromInputStream(getClass.getResourceAsStream(args("inputFirstEntities"))).getLines.toList

  private val secondEntities =
    Source.fromInputStream(getClass.getResourceAsStream(args("inputSecondEntities"))).getLines.toList

  private val triples = TextLine(args("input")).read.mapTo('line ->('subject, 'predicate, 'object)) {
    line: String =>
      val nodes = extractNodes(line)
      (nodes._2, nodes._3, nodes._4)
  }.filter(('subject, 'object)) {
    fields: (Subject, Range) =>
      (firstEntities.contains(fields._1) && secondEntities.contains(fields._2)) ||
        (firstEntities.contains(fields._2) && secondEntities.contains(fields._1))
  }

  triples.project(('predicate)).
    groupBy('predicate) {
    _.size('count)
  }.groupAll {
    _.sortBy('count).reverse
  }.project(('predicate, 'count)).write(Tsv(args("output")))
}
