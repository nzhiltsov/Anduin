package ru.ksu.niimm.cll.anduin

import com.twitter.scalding.{TextLine, Job, Args}
import util.FixedPathLzoTsv

/**
 * Given a list of unique entity URIs ("candidates"),
 * this processor filters out non-relevant relations from the adjacency list
 *
 * @author Nikita Zhiltsov
 *
 * @deprecated this tool must be merged with AdjacencyListProcessor
 */
class AdjacencyOfCandidateEntitiesProcessor(args: Args) extends Job(args) {
  private val candidateEntities =
    new TextLine(args("inputCandidateList")).read.map('line -> 'line) {
      line: String => line.mkString("<", "", ">")
    }

  private val triples =
    new TextLine(args("input")).read.unique('line).
      mapTo('line ->('relPredicateId, 'subject, 'object)) {
      line: String =>
        val triple = line.split('\t')
        (triple(0), triple(1), triple(2))
    }

  private val filteredTriples =
    triples.joinWithSmaller('subject -> 'line, candidateEntities).project(('relPredicateId, 'subject, 'object))
      .joinWithSmaller('object -> 'line, candidateEntities).project(('relPredicateId, 'subject, 'object))

  filteredTriples
    .unique(('relPredicateId, 'subject, 'object))
    .groupAll {
    _.sortBy(('relPredicateId, 'subject))
  }.write(new FixedPathLzoTsv(args("output")))
}
