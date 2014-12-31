package ru.ksu.niimm.cll.anduin.entitysearch

import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.NodeParser.Range
import com.twitter.scalding.Tsv
import ru.ksu.niimm.cll.anduin.util.PredicateGroupCodes._

/**
 * This processor resolves identity links according to S1_1 model (owl:sameAs, dbpedia:disambiguates, dbpedia:redirect)
 * from the paper
 * Tonon, A., Demartini, G., & Cudré-Mauroux, P. (2012). Combining inverted indices and structured search
 * for ad-hoc object retrieval. Proceedings of the 35th International ACM SIGIR Conference on Research
 * and Development in Information Retrieval.
 *
 * @author Nikita Zhiltsov 
 */
class IdentityLinkProcessor(args: Args) extends Job(args) {
  private val inputFormat = args("inputFormat")

  def isNquad = inputFormat.equals("nquad")

  private val entityNames =
    TypedTsv[(String, String)](args("entityNames")).read.rename((0, 1) ->('entityUri, 'names))

  private val firstLevelEntities =
    TextLine(args("input")).read
      .filter('line) {
      line: String =>
        val cleanLine = line.trim
        cleanLine.startsWith("<")
    }
      .mapTo('line ->('subject, 'predicate, 'object)) {
      line: String =>
        if (isNquad) extractTripleFromQuad(line) else extractNodesFromN3(line)
    }
      .filter(('subject, 'object))(retainOnlyObjectLinks)

  private val sameAsLinks = firstLevelEntities.filter('predicate)(retainOnlySameAsLinks).project(('subject, 'object))

  private val disambiguatesLinks = firstLevelEntities.filter('predicate)(retainOnlyDisambiguatesLinks).project(('subject, 'object))

  private val redirectLinks = firstLevelEntities.filter('predicate)(retainOnlyRedirectLinks).project(('subject, 'object))

  private val outgoingSameAsAttributes = sameAsLinks.joinWithLarger(('object -> 'entityUri), entityNames)
    .project(('subject, 'names))

  private val incomingRedirectAttributes = redirectLinks.joinWithLarger(('subject -> 'entityUri), entityNames)
    .project(('object, 'names)).rename('object -> 'subject).project(('subject, 'names))

  private val incomingSameAsAttributes = sameAsLinks.joinWithLarger(('subject -> 'entityUri), entityNames)
    .project(('object, 'names)).rename('object -> 'subject).project(('subject, 'names))

  private val outgoingDisambiguatesAttributes = disambiguatesLinks.joinWithLarger(('object -> 'entityUri), entityNames)
    .project(('subject, 'names))

  private def retainOnlyObjectLinks(fields: (Subject, Range)): Boolean = fields match {
    case (subject, range) => subject.startsWith("<") && range.startsWith("<")
  }

  private def retainOnlySameAsLinks(predicate: Predicate): Boolean = predicate.equals(OWL_SAMEAS_PREDICATE)

  private def retainOnlyDisambiguatesLinks(predicate: Predicate): Boolean = predicate.equals(DBPEDIA_DISAMBIGUATES_PREDICATE)

  private def retainOnlyRedirectLinks(predicate: Predicate): Boolean = predicate.equals(DBPEDIA_REDIRECT_PREDICATE)

  private val mergedDescriptions = incomingSameAsAttributes ++ outgoingSameAsAttributes ++ outgoingDisambiguatesAttributes ++ incomingRedirectAttributes

  mergedDescriptions.groupBy('subject) {
    _.mkString('names, " ")
  }
  .mapTo(('subject, 'names) -> ('predicatetype, 'subject, 'names)) {
    fields: (Subject, Range) =>
      (SIMILAR_ENTITY_NAMES, fields._1, fields._2)
  }
  .write(Tsv(args("output")))

}
