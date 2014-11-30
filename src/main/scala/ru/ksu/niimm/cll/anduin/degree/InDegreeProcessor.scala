package ru.ksu.niimm.cll.anduin.degree

import com.twitter.scalding.{Job, Args, TextLine, Tsv}
import ru.ksu.niimm.cll.anduin.util.NodeParser._

/**
 * @author Nikita Zhiltsov 
 */
class InDegreeProcessor(args: Args) extends Job(args) {
  private val inputFormat = args("inputFormat")

  def isNquad = inputFormat.equals("nquad")
  /**
   * reads the entity triples
   */
  private val triples = TextLine(args("input")).read.mapTo('line ->('subject, 'predicate, 'object)) {
    line: String =>
      if (isNquad) {
        val nodes = extractNodes(line)
        (nodes._2, nodes._3, nodes._4)
      } else extractNodesFromN3(line)
  }

  triples.filter('object) {
    range: Range => range.startsWith("<")
  }.unique(('subject, 'predicate, 'object))
    .groupBy(('object)) {
    _.size
  }.map('size -> 'size) {
    size: String => Integer.parseInt(size)
  }
    .groupAll {
    _.sortBy('size).reverse
  }.write(Tsv(args("output")))
}
