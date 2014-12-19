package ru.ksu.niimm.cll.anduin.entitysearch

import com.twitter.scalding.{Tsv, TypedTsv, Job, Args}
import ru.ksu.niimm.cll.anduin.util.NodeParser._

/**
 * This processor resolves outgoing links in entity descriptions
 *
 * @author Nikita Zhiltsov 
 */
class OutgoingLinkProcessor(args: Args) extends Job(args) {
  private val firstLevelEntities =
    TypedTsv[(String, Subject, Range)](args("inputFirstLevel")).read.rename((0, 1, 2) ->('predicateid, 'subject, 'object))

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
    .joinWithSmaller(('object -> 'subject2), secondLevelEntities)
    .project('predicateid, 'subject, 'object2)
    .map('predicateid -> 'predicateid) {
    predicateType: String => 1
  }
    .write(Tsv(args("output")))
}
