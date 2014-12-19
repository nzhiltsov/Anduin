package ru.ksu.niimm.cll.anduin.description

import com.twitter.scalding.{TextLine, Job, Args}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.PredicateGroupCodes._
import com.twitter.scalding._
/**
 * @author Nikita Zhiltsov 
 */
class NamesProcessor(args: Args) extends Job(args) {

  private val triples = TextLine(args("input")).read
    .filter('line) {
    line: String =>
      val cleanLine = line.trim
      cleanLine.startsWith("<")
  }
    .mapTo('line ->('subject, 'predicate, 'object))(extractNodesFromN3)

  /**
   * filters first level entities with literal values
   */
  private val aggregatedNames = triples.filter('object) {
    range: ru.ksu.niimm.cll.anduin.util.NodeParser.Range => !range.startsWith("<")
  }
    .filter('object) {
    range: ru.ksu.niimm.cll.anduin.util.NodeParser.Range =>
      if (range.startsWith("\"") && !range.endsWith("\"")) range.endsWith("@en") else true
  }
    .map('predicate -> 'predicatetype) {
    predicate: Predicate => encodePredicateType(predicate, true)
  }
    .filter('predicatetype) {
    predicateType: Int => predicateType == NAMES
  }
    .project(('subject, 'object))
    .groupBy('subject) {
    _.mkString('object, " ")
  }

  aggregatedNames
    .project(('subject, 'object))
    .map('object -> 'object) {
    range: ru.ksu.niimm.cll.anduin.util.NodeParser.Range =>
      cleanHTMLMarkup(range)
  }
    .write(Tsv(args("output")))
}
