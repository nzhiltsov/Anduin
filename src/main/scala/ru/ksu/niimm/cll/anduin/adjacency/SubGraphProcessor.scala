package ru.ksu.niimm.cll.anduin.adjacency

import com.twitter.scalding.{Tsv, TextLine, Job, Args}
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTextLine
import ru.ksu.niimm.cll.anduin.util.NodeParser._

/**
 * Given a list of entities,
 * this processor outputs the subgraph, which connects the entities between each other
 *
 * @author Nikita Zhiltsov 
 */
class SubGraphProcessor(args: Args) extends Job(args) {
  private val inputFormat = args("inputFormat")

  def isNquad = inputFormat.equals("nquad")

  private val relevantEntities = TextLine(args("inputEntities")).read.rename('line -> 'entityURI)

  /**
   * reads the entity subgraph
   */
  private val subgraph = TextLine(args("input")).read.filter('line) {
    line: String =>
      val cleanLine = line.trim
      cleanLine.startsWith("<")
  }.mapTo('line ->('subject, 'predicate, 'object)) {
    line: String =>
      if (isNquad) {
        val nodes = extractNodes(line)
        (nodes._2, nodes._3, nodes._4)
      } else extractNodesFromN3(line)
  }.filter(('subject, 'object)) {
    fields: (Subject, Range) =>
      fields._1.startsWith("<") && fields._2.startsWith("<")
  }.joinWithSmaller('subject -> 'entityURI, relevantEntities)
  .project(('subject, 'predicate, 'object))
  .joinWithSmaller('object -> 'entityURI, relevantEntities)
  .project(('subject, 'predicate, 'object))
  .map('object -> 'object) {
    range: String => range + " ."
  }

  subgraph
    .write(Tsv(args("output")))
}
