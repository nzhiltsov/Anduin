package ru.ksu.niimm.cll.anduin.degree

import com.twitter.scalding.{Job, Args}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import com.twitter.scalding.Tsv
import com.twitter.scalding.TextLine

/**
 * This processor computes out degrees of entities in the RDF graph.
 *  The output is as follows:
 *  <p>
 * <b>entity_uri TAB out degree</b>
 * </p>
 *
 * @author Nikita Zhiltsov 
 */
class OutDegreeProcessor(args: Args) extends Job(args) {
  /**
   * reads the entity triples
   */
  private val triples = TextLine(args("input")).read.mapTo('line ->('subject, 'predicate, 'object)) {
    line: String =>
      val nodes = extractNodes(line)
      (nodes._2, nodes._3, nodes._4)
  }

  triples.filter('subject) {
    subject: Subject => subject.startsWith("<")
  }.unique(('subject, 'predicate, 'object))
    .groupBy(('subject)) {
    _.size
  }.map('size -> 'size) {
    size: String => Integer.parseInt(size)
  }
    .groupAll {
    _.sortBy('size).reverse
  }.write(Tsv(args("output")))
}
