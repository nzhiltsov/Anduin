package ru.ksu.niimm.cll.anduin.entitytype

import com.twitter.scalding.{Tsv, Job, Args, TypedTsv}

/**
 * Given a list of entities and list of types for all entities,
 * this processor outputs the list of entity types only for the given entities
 *
 * @author Nikita Zhiltsov 
 */
class EntityTypeFilterProcessor(args: Args) extends Job(args) {
  private val relevantEntities =
    TypedTsv[(Int, String)](args("inputEntityList")).read.rename((0, 1) ->('relEntityId, 'relEntityURI))

  private val entityWithTypes = TypedTsv[(String, String)](args("input")).read.rename((0, 1) ->('entityURI, 'types))

  relevantEntities.joinWithLarger('relEntityURI -> 'entityURI, entityWithTypes)
    .project(('relEntityId, 'relEntityURI, 'types))
    .filter('relEntityId) {
    // todo: a workaround, don't realize why there are empty entries after inner join
    field: String =>
      field != null &&
        !field.trim.isEmpty
  }
    .groupAll {
    _.sortBy('relEntityId)
  }
    .write(Tsv(args("output")))
}
