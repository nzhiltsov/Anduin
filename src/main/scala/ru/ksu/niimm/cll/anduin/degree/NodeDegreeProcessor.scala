package ru.ksu.niimm.cll.anduin.degree

import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.{FixedPathLzoTsv, FixedPathLzoTextLine}
import com.twitter.scalding.Tsv
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import com.twitter.scalding.Tsv
import com.twitter.scalding.Tsv

/**
 * Given a list of triples (predicateId, subjectURI, objectURI),
 * this processor outputs the entityURI node degrees (i.e., the number of adjacent incoming links as well as outgoing links)
 *
 * @author Nikita Zhiltsov 
 */
class NodeDegreeProcessor(args: Args) extends Job(args) {
  private val includeDatatype = java.lang.Boolean.parseBoolean(args("includeDatatype"))

  /**
   * reads the entity triples
   */
  private val triples =
    TextLine(args("input")).read.filter('line) {
    line: String =>
      val cleanLine = line.trim
      cleanLine.startsWith("<")
  }
    .mapTo('line ->('subject, 'predicate, 'object))(extractNodesFromN3)
    .filter('object) {
    range: String => if (includeDatatype) true else range.startsWith("<")
  }.unique(('subject, 'predicate, 'object))

  private val subjects = triples.project(('subject))

  private val objects = triples.project(('object)).filter('object){
    range: String => range.startsWith("<")
  }.rename('object -> 'subject)

  private val nodes = subjects ++ objects

  nodes.groupBy(('subject)) {
    _.size
  }.map('size -> 'size) {
    size: String => Integer.parseInt(size)
  }.groupAll {
    _.sortBy('size).reverse
  }.write(Tsv(args("output")))
}
