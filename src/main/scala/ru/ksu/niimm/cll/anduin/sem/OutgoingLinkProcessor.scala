package ru.ksu.niimm.cll.anduin.sem

import com.twitter.scalding.{Tsv, TypedTsv, Job, Args}
import cascading.pipe.joiner.LeftJoin
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTextLine

/**
 * This processor resolves outgoing links in entity descriptions
 *
 * @author Nikita Zhiltsov 
 */
class OutgoingLinkProcessor(args: Args) extends Job(args) {
  private val firstLevelEntities =
    new FixedPathLzoTextLine(args("inputFirstLevel")).read.filter('line) {
      line: String =>
        val cleanLine = line.trim
        cleanLine.startsWith("<")
    }
      .mapTo('line ->('subject, 'object)) {
      line: String =>
        val triple = extractNodesFromN3(line)
        (triple._1, triple._3)
    }
      .filter(('subject, 'object)) {
      fields: (Subject, Range) => fields._1.startsWith("<") && fields._2.startsWith("<")
    }

  /**
   * filters second level entities with URIs as objects, 'name'-like predicates and literal values;
   * merges the literal values
   */
  private val secondLevelEntities =
    TypedTsv[(String, Subject, Range)](args("inputSecondLevel")).read.rename((0, 1, 2) ->('predicatetype, 'subject2, 'object2))
      .filter('predicatetype) {
      predicateType: String => predicateType.equals("0")
    }
      .project(('subject2, 'object2))

  /**
   * resolves URIs as objects across the whole data set and saves the data
   */
  firstLevelEntities
    .joinWithSmaller(('object -> 'subject2), secondLevelEntities, joiner = new LeftJoin)
    .mapTo(('subject, 'object, 'subject2, 'object2) ->('predicatetype, 'subject, 'object)) {
    fields: (Subject, Range, Subject, Range) =>
      if (fields._4 == null) (2, fields._1, stripURI(fields._2).mkString("\"", "", "\"")) else (2, fields._1, fields._4)
  }
    .write(Tsv(args("output")))
}
