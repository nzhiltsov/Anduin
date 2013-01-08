package ru.ksu.niimm.cll.anduin

import com.twitter.scalding.{TextLine, Job, Args}
import util.{FixedPathLzoTextLine, FixedPathLzoTsv}
import cascading.pipe.joiner.InnerJoin

/**
 * Given a list of unique entity URIs ("candidates"),
 * this processor filters out non-relevant relations from the adjacency list
 *
 * @author Nikita Zhiltsov 
 */
class AdjacencyOfCandidateEntitiesProcessor(args: Args) extends Job(args) {
  private val candidateEntities = new FixedPathLzoTextLine(args("inputCandidateList")).read

  private val triples =
    new TextLine(args("input")).read.unique('line).
      mapTo('line ->('relPredicateId, 'subject, 'object)) {
      line: String =>
        val triple = line.split('\t')
        (triple(0), triple(1), triple(2))
    }

  private val filteredBySubjectTriples =
    candidateEntities.joinWithLarger('line -> 'subject, triples, joiner = new InnerJoin)
      .project(('relPredicateId, 'subject, 'object))

  private val filteredByRangeTriples =
    candidateEntities.joinWithLarger('line -> 'object, triples, joiner = new InnerJoin)
      .project(('relPredicateId, 'subject, 'object))

  private val filteredTriples = filteredBySubjectTriples ++ filteredByRangeTriples

  filteredTriples
    .unique(('relPredicateId, 'subject, 'object))
    .groupAll {
    _.sortBy(('relPredicateId, 'subject))
  }.write(new FixedPathLzoTsv(args("output")))
}
