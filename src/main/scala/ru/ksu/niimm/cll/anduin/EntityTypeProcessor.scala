package ru.ksu.niimm.cll.anduin

import com.twitter.scalding.{TypedTsv, Job, Args}
import util.FixedPathLzoTsv
import util.NodeParser._
import com.twitter.scalding.TextLine
import cascading.pipe.joiner.InnerJoin

/**
 * Given a list of entity types (see top-50 BTC classes) and list of entities,
 * this processor outputs the types of each entity
 *
 * @author Nikita Zhiltsov 
 */
class EntityTypeProcessor(args: Args) extends Job(args) {
  val RDF_TYPE_PREDICATE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
  /**
   * reads the predicates of interest
   */
  private val relevantTypes =
    TypedTsv[(String, Int)](args("inputTypeList")).read.rename((0, 1) ->('relType, 'relTypeId))

  private val relevantEntities = new TextLine(args("inputEntityList")).read.rename('line -> 'relEntityUri)

  private val typeStatements = new TextLine(args("input")).read.mapTo('line ->('context, 'subject, 'predicate, 'object)) {
    line: String => extractNodes(line)
  }.project(('subject, 'predicate, 'object)).filter(('subject, 'predicate)) {
    st: (Subject, Predicate) =>
      st._1.startsWith("<") && st._2.equals(RDF_TYPE_PREDICATE)
  }

  val entityWithTypes = typeStatements.joinWithTiny('object -> 'relType, relevantTypes)
    .project(('subject, 'relTypeId))
    .unique(('subject, 'relTypeId))
    .groupBy('subject) {
    _.mkString('relTypeId, ", ")
  }

  entityWithTypes.joinWithSmaller('subject -> 'relEntityUri, relevantEntities, new InnerJoin)
    .project(('subject, 'relTypeId))
    .write(new FixedPathLzoTsv(args("output")))
}
