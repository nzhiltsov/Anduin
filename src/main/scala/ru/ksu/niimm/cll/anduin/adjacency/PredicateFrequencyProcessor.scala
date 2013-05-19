package ru.ksu.niimm.cll.anduin.adjacency

import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import com.twitter.scalding.TextLine

/**
 * Given a list of entities,
 * this processor outputs frequencies of predicates between these entities
 *
 * @author Nikita Zhiltsov 
 */
class PredicateFrequencyProcessor(args: Args) extends Job(args) {
  private val relevantEntities = TextLine(args("inputEntities")).read.rename('line -> 'entityUri1).project('entityUri1)

  private val sameRelevantEntities = relevantEntities.rename('entityUri1 -> 'entityUri2)

  private val entityPairs = relevantEntities.crossWithTiny(sameRelevantEntities)

  private val triples = TextLine(args("input")).read.mapTo('line ->('subject, 'predicate, 'object)) {
    line: String =>
      val nodes = extractNodes(line)
      (nodes._2, nodes._3, nodes._4)
  }.filter(('subject, 'object)) {
    fields: (Subject, Range) =>
      fields._1.startsWith("<") && fields._2.startsWith("<")
  }.unique(('subject, 'predicate, 'object))

  triples.joinWithTiny(('subject, 'object) -> ('entityUri1, 'entityUri2), entityPairs)
    .project(('predicate)).
    groupBy('predicate) {
    _.size('count)
  }.groupAll {
    _.sortBy('count).reverse
  }.project(('predicate, 'count)).write(Tsv(args("output")))
}
