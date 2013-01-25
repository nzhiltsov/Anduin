package ru.ksu.niimm.cll.anduin

import com.twitter.scalding._
import util.NodeParser._
import com.twitter.scalding.TextLine

/**
 * Given a list of entity types (see e.g. top-200 BTC classes) and list of entities,
 * this processor outputs the types of each entity
 *
 * @author Nikita Zhiltsov 
 */
class EntityTypeProcessor(args: Args) extends Job(args) {
  val RDF_TYPE_PREDICATE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
  private val wordEntityPairs = TypedTsv[(String, String)](args("inputTermEntityPairs")).read.rename((0, 1) ->('word, 'entityUri))
  /**
   * reads the predicates of interest
   */
  private val relevantTypes =
    TypedTsv[(String, Int)](args("inputTypeList")).read.rename((0, 1) ->('relType, 'relTypeId))

  private val typeStatements = new TextLine(args("input")).read.mapTo('line ->('context, 'subject, 'predicate, 'object)) {
    line: String => extractNodes(line)
  }.project(('subject, 'predicate, 'object)).filter(('subject, 'predicate)) {
    st: (Subject, Predicate) =>
      st._1.startsWith("<") && st._2.equals(RDF_TYPE_PREDICATE)
  }

  val entityWithTypes = typeStatements.joinWithTiny('object -> 'relType, relevantTypes)
    .project(('subject, 'relTypeId))
    .unique(('subject, 'relTypeId))

  val wordWithTypes = entityWithTypes.joinWithLarger('subject -> 'entityUri, wordEntityPairs)
    .project(('word, 'relTypeId))

  wordWithTypes.groupBy(('word, 'relTypeId)) {
    _.reducers(10).size('count)
  }.write(Tsv(args("output")))

}
