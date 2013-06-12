package ru.ksu.niimm.cll.anduin.description

import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import com.twitter.scalding.TextLine

/**
 * Given a list of entity URIs,
 * this processor, for each entity, groups all the triples that have the entity as a subject.
 *
 * @author Nikita Zhiltsov 
 */
class EntityDescriptionProcessor(args: Args) extends Job(args) {

  private val relevantEntities = TextLine(args("inputEntities")).read.rename('line -> 'entityURI)

  private val quads = new TextLine(args("input")).read
    .mapTo('line ->('subject, 'predicate, 'object)) {
    line: String =>
      val quad = extractNodes(line)
      (quad._2, quad._3, quad._4)
  }

 quads.joinWithTiny('subject -> 'entityURI, relevantEntities).project(('subject, 'predicate, 'object))
  .unique(('subject, 'predicate, 'object))
   .groupAll {
   _.sortBy(('subject, 'predicate))
 }
   .write(Tsv(args("output")))
}
