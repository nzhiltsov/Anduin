package ru.ksu.niimm.cll.anduin.adjacency

import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTextLine

/**
 * This processor outputs unique predicates (object properties) with their frequencies for the whole collection
 *
 * @author Nikita Zhiltsov 
 */
class CollectionPredicateFrequencyProcessor(args: Args) extends Job(args) {
  private val inputFormat = args("inputFormat")

  def isNquad = inputFormat.equals("nquad")

  /**
   * reads the triples
   */
  private val triples = new FixedPathLzoTextLine(args("input")).read.filter('line){
    line: String =>
      val cleanLine = line.trim
      cleanLine.startsWith("<") || cleanLine.startsWith("_")
  }
    .mapTo('line ->('subject, 'predicate, 'object)) {
    line: String =>
      if (isNquad) {
        val nodes = extractNodes(line)
        (nodes._2, nodes._3, nodes._4)
      } else extractNodesFromN3(line)
  }.filter(('subject, 'object)) {
    fields: (Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range) =>
      fields._1.startsWith("<") && fields._2.startsWith("<")
  }.unique(('subject, 'predicate, 'object))

  triples.groupBy('predicate) {
    _.size('count)
  }.groupAll {
    _.sortBy('count).reverse
  }.project(('predicate, 'count)).write(Tsv(args("output")))
}
