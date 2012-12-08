package ru.ksu.niimm.cll.anduin

import com.twitter.scalding.{Tsv, Args}
import ru.ksu.niimm.cll.anduin.NodeParser._
import cascading.pipe.joiner.LeftJoin

/**
 * This processor implements aggregation of entity description with partial URI resolution according to the paper
 * Neumayer, R., Balog, K. On the Modeling of Entities for Ad-Hoc Entity Search in the Web of Data. ECIR'12
 * <br/>
 * The output format is as follows:
 * <p>
 * <b>predicate_type TAB subject TAB predicate TAB objects</b>
 * </p>
 * where 'predicate_type' values are {0,1,2}
 * and 'objects' may contain more than one literals delimited with space, e.g.
 * <p>
 * <b>2	<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>	<http://www.aktors.org/ontology/portal#has-author>	"Tyson" "Johnson"</b>
 * </p>
 @author Nikita Zhiltsov
 */
class SEMProcessor(args: Args) extends AbstractProcessor(args) {
  val wordDelimiterRegex = "[^a-zA-Z]"

  private val secondLevelEntities =
    firstLevelEntities.rename(('context, 'subject, 'predicate, 'object) ->
      ('context2, 'subject2, 'predicate2, 'object2)).filter(('subject2, 'predicate2, 'object2)) {
      fields: (Subject, Predicate, Range) => fields._1.startsWith("<") && fields._3.startsWith("\"") && isNamePredicate(fields._2)
    }

  private val secondLevelBNodes = firstLevelEntities.rename(('context, 'subject, 'predicate, 'object) ->
    ('context3, 'subject3, 'predicate3, 'object3)).filter(('subject3, 'predicate3, 'object3)) {
    fields: (Subject, Predicate, Range) => fields._1.startsWith("_") && fields._3.startsWith("\"") && isNamePredicate(fields._2)
  }

  def isNamePredicate(predicate: Predicate): Boolean =
    predicate.matches("^<http.*(label|name|title)>$")

  private val mergedEntities = firstLevelEntitiesWithoutBNodes
    .joinWithSmaller(('context, 'object) ->('context3, 'subject3), secondLevelBNodes, joiner = new LeftJoin)
    .joinWithSmaller(('object -> 'subject2), secondLevelEntities, joiner = new LeftJoin)
    .project(('subject, 'predicate, 'object, 'object2, 'object3))
    .map(('object, 'object2, 'object3) -> (('objects, 'predicatetype))) {
    fields: (Range, Range, Range) =>
      if (fields._2 != null) {
        (fields._2, 2)
      } else if (fields._3 != null) {
        (fields._3, 2)
      } else {
        (fields._1, 1)
      }
  }
    .project(('predicatetype, 'subject, 'predicate, 'objects))
    .filter('objects) {
    range: Range => !range.startsWith("_")
  }
    .map('objects -> 'objects) {
    range: Range =>
      if (!range.startsWith("<")) range
      else
        range.split(wordDelimiterRegex).mkString("\"", " ", "\"")
  }
    .groupBy(('predicatetype, 'subject, 'predicate)) {
    _.mkString('objects, " ")
  }.map(('predicatetype, 'predicate) ->('predicatetype, 'predicate)) {
    fields: (Int, Predicate) =>
      if (fields._1 == 2) (fields._1, fields._2)
      else if (isNamePredicate(fields._2)) (0, fields._2)
      else (1, fields._2)
  }

  mergedEntities.
    groupAll {
    _.sortBy('subject)
  }
    .write(Tsv(args("output")))
}
