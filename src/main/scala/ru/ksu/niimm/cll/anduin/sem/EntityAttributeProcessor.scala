package ru.ksu.niimm.cll.anduin.sem

import com.twitter.scalding.{TypedTsv, Job, Args, Tsv}
import ru.ksu.niimm.cll.anduin.util.NodeParser._

/**
 * Given an input of quads,
 * this processor outputs the values of entity attributes distinguishing 'name'-like attributes

 * @author Nikita Zhiltsov 
 */
class EntityAttributeProcessor(args: Args) extends Job(args) {
  private val firstLevelEntities =
    TypedTsv[(Context, Subject, Predicate, Range)](args("input")).read.rename((0, 1, 2, 3) ->('context, 'subject, 'predicate, 'object))
  /**
   * filters first level entities with URIs as subjects, i.e. candidates for further indexing
   */
  private val firstLevelEntitiesWithoutBNodes = firstLevelEntities.filter('subject) {
    subject: Subject => subject.startsWith("<")
  }

  /**
   * filters first level entities with literal values
   */
  private val firstLevelEntitiesWithLiterals = firstLevelEntitiesWithoutBNodes.filter('object) {
    range: Range => range.startsWith("\"")
  }.project(('subject, 'predicate, 'object)).unique(('subject, 'predicate, 'object)).groupBy(('subject, 'predicate)) {
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
