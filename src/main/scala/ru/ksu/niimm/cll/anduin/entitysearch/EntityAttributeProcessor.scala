package ru.ksu.niimm.cll.anduin.entitysearch

import com.twitter.scalding.{Job, Args, Tsv}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTextLine

/**
 * Given an input of quads,
 * this processor outputs the values of entity attributes distinguishing 'name'-like attributes

 * @author Nikita Zhiltsov 
 */
class EntityAttributeProcessor(args: Args) extends Job(args) {
  private val inputFormat = args("inputFormat")

  def isNquad = inputFormat.equals("nquad")

  private val firstLevelEntitiesWithoutBNodes = new FixedPathLzoTextLine(args("input")).read
    .filter('line) {
    line: String =>
      val cleanLine = line.trim
      cleanLine.startsWith("<")
  }
    .mapTo('line ->('subject, 'predicate, 'object)) {
    line: String =>
      if (isNquad) {
        val nodes = extractNodes(line)
        (nodes._2, nodes._3, nodes._4)
      } else extractNodesFromN3(line)
  }

  /**
   * filters first level entities with literal values
   */
  private val firstLevelEntitiesWithLiterals = firstLevelEntitiesWithoutBNodes.filter('object) {
    range: Range => range.startsWith("\"")
  }.unique(('subject, 'predicate, 'object)).groupBy(('subject, 'predicate)) {
    _.mkString('object, " ")
  }.map('predicate -> 'predicatetype) {
    predicate: Predicate => if (isNamePredicate(predicate)) 0 else 1
  }
    .project(('predicatetype, 'subject, 'object))

  firstLevelEntitiesWithLiterals
    .unique(('predicatetype, 'subject, 'object))
    .map('object -> 'object) {
    range: Range =>
      cleanHTMLMarkup(range)
  }
    .write(Tsv(args("output")))
}
