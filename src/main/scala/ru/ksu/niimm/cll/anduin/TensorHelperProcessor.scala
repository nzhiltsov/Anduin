package ru.ksu.niimm.cll.anduin

import com.twitter.scalding.{Tsv, TypedTsv, Job, Args}

/**
 * Given an adjacency list (see the output of [[ru.ksu.niimm.cll.anduin.adjacency.AdjacencyListProcessor]]]),
 * this processor outputs the tensor entries (first - rows, then - columns) sorted by their slice indices (i.e., predicateId)
 *
 * @author Nikita Zhiltsov 
 */
class TensorHelperProcessor(args: Args) extends Job(args) {

  private val adjacencyList = TypedTsv[(String, String, String)](args("input")).read.rename((0, 1, 2) ->('predicateId, 'subject, 'object))

  private val entities =
    TypedTsv[(String, String)](args("inputEntityList")).read.rename((0, 1) ->('entityId, 'entityURI))

  private val entities2 =
    TypedTsv[(String, String)](args("inputEntityList")).read.rename((0, 1) ->('entityId, 'entityURI))

  val tensorEntries = adjacencyList.joinWithSmaller('subject -> 'entityURI, entities)
    .project(('predicateId, 'entityId, 'object)).rename('entityId -> 'subjectId)
    .joinWithSmaller('object -> 'entityURI, entities2).project(('predicateId, 'subjectId, 'entityId)).rename('entityId -> 'objectId)
    .unique(('predicateId, 'subjectId, 'objectId))

  tensorEntries.groupAll {
    _.sortBy('predicateId)
  }.write(Tsv(args("output")))

}
