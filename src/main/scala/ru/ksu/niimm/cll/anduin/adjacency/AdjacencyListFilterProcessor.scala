package ru.ksu.niimm.cll.anduin.adjacency

import com.twitter.scalding._
import com.twitter.scalding.TextLine

/**
 * Given a list of entities and list of predicates,
 * this processor filters entries related to them.
 *
 * @author Nikita Zhiltsov 
 */
class AdjacencyListFilterProcessor(args: Args) extends Job(args) {
  private val relevantEntities = TextLine(args("inputEntities")).read.rename('line -> 'entityURI)

  private val relevantPredicates =
    TypedTsv[(String, String)](args("inputPredicates")).read.rename((0, 1) ->('relPredicate, 'relPredicateId))

  private val edges = TypedTsv[(String, String, String)](args("input"))
    .read.rename((0, 1, 2) ->('predicateId, 'subject, 'object))
    .joinWithTiny('predicateId -> 'relPredicateId, relevantPredicates).project(('relPredicateId, 'subject, 'object))

  private val subjectEntries = edges.joinWithSmaller('subject -> 'entityURI, relevantEntities).project(('relPredicateId, 'subject, 'object))

  private val objectEntries = edges.joinWithSmaller('object -> 'entityURI, relevantEntities).project(('relPredicateId, 'subject, 'object))

  private val filteredEntries = subjectEntries ++ objectEntries

  filteredEntries.unique(('relPredicateId, 'subject, 'object)).write(Tsv(args("output")))
}
