package ru.ksu.niimm.cll.anduin.entitytype

import com.twitter.scalding.{Job, Args}
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTsv
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import com.twitter.scalding.TextLine

/**
 * This processor outputs unique classes (entity types) with their frequencies
 *
 * @author Nikita Zhiltsov 
 */
class EntityTypeFrequencyProcessor(args: Args) extends Job(args) {
  val RDF_TYPE_PREDICATE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#entitytype>"
  /**
   * reads the triples
   */
  private val objects = TextLine(args("input")).read.mapTo('line ->('subject, 'predicate, 'object)) {
    line: String =>
      val nodes = extractNodes(line)
      (nodes._2, nodes._3, nodes._4)
  }.filter(('predicate, 'object)) {
    fields: (Predicate, Range) =>
      fields._1.equals(RDF_TYPE_PREDICATE) && fields._2.startsWith("<")
  }.project(('subject, 'object))
    .unique(('subject, 'object))
    .project('object)

  objects.groupBy('object) {
    _.size('count)
  }.groupAll {
    _.sortBy('count).reverse
  }.project(('object, 'count)).write(new FixedPathLzoTsv(args("output")))
}
