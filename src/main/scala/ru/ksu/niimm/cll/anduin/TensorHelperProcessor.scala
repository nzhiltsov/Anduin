package ru.ksu.niimm.cll.anduin

import com.twitter.scalding.{TypedTsv, Job, Args}
import util.FixedPathLzoTsv

/**
 * Given an adjacency list (see the output of [[ru.ksu.niimm.cll.anduin.AdjacencyOfCandidateEntitiesProcessor]]]),
 * this processor outputs the tensor entries (first - columns, then - rows) sorted by their slice indices (i.e., predicateId)
 *
 * @author Nikita Zhiltsov 
 */
class TensorHelperProcessor(args: Args) extends Job(args) {

  private val adjacencyList = TypedTsv[(Int, String, String)](args("input")).read.rename((0, 1, 2) ->('predicateId, 'subject, 'object))

  private val entities = TypedTsv[(String, Int)](args("inputEntityList")).read.rename((0, 1) ->('entityURI, 'entityId))

  val tensorEntries = adjacencyList.joinWithSmaller('subject -> 'entityURI, entities)
    .project(('predicateId, 'entityId, 'object)).rename('entityId -> 'subjectId)
    .joinWithSmaller('object -> 'entityURI, entities).project(('predicateId, 'subjectId, 'entityId)).rename('entityId -> 'objectId)
    .unique(('predicateId, 'subjectId, 'objectId))

  tensorEntries.groupAll {
    _.sortBy('predicateId)
  }.write(new FixedPathLzoTsv(args("output")))

}
