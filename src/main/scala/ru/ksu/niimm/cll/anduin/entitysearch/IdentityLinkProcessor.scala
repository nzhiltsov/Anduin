package ru.ksu.niimm.cll.anduin.entitysearch

import com.twitter.scalding.{Tsv, TypedTsv, Job, Args}
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTextLine
import ru.ksu.niimm.cll.anduin.util.NodeParser._

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
  private val entityAttributes =
    TypedTsv[(Int, Subject, Range)](args("inputEntityAttributes")).read
      .rename((0, 1, 2) ->('predicatetype, 'attrsubject, 'content))
      .map('predicatetype -> 'predicatetype) {
      predicateType: Int => predicateType + 2
    }

  private val MAX_LINE_LENGTH = 100000

  private val firstLevelEntities =
    new FixedPathLzoTextLine(args("inputGraph")).read.filter('line) {
      line: String =>
        val cleanLine = line.trim
        cleanLine.startsWith("<") && cleanLine.length < MAX_LINE_LENGTH
    }
      .mapTo('line ->('subject, 'predicate, 'object))(extractTripleFromQuad)
      .filter(('subject, 'object))(retainOnlyObjectLinks)

  private val sameAsLinks = firstLevelEntities.filter('predicate)(retainOnlySameAsLinks).project(('subject, 'object))

  private val disambiguatesLinks = firstLevelEntities.filter('predicate)(retainOnlyDisambiguatesLinks).project(('subject, 'object))

  private val redirectLinks = firstLevelEntities.filter('predicate)(retainOnlyRedirectLinks).project(('subject, 'object))

  private val outgoingSameAsAttributes = sameAsLinks.joinWithLarger(('object -> 'attrsubject), entityAttributes)
    .project(('predicatetype, 'subject, 'content))

  private val outgoingRedirectAttributes = redirectLinks.joinWithLarger(('object -> 'attrsubject), entityAttributes)
    .project(('predicatetype, 'subject, 'content))

  private val incomingSameAsAttributes = sameAsLinks.joinWithLarger(('subject -> 'attrsubject), entityAttributes)
    .project(('predicatetype, 'object, 'content)).rename('object -> 'subject).project(('predicatetype, 'subject, 'content))

  private val incomingDisambiguatesAttributes = disambiguatesLinks.joinWithLarger(('subject -> 'attrsubject), entityAttributes)
    .project(('predicatetype, 'object, 'content)).rename('object -> 'subject).project(('predicatetype, 'subject, 'content))

  private def retainOnlyObjectLinks(fields: (Subject, Range)): Boolean = fields match {
    case (subject, range) => subject.startsWith("<") && range.startsWith("<")
  }

  private val OWL_SAMEAS_PREDICATE = "<http://www.w3.org/2002/07/owl#sameAs>"
  private val DBPEDIA_DISAMBIGUATES_PREDICATE = "<http://dbpedia.org/property/disambiguates>"
  private val DBPEDIA_REDIRECT_PREDICATE = "<http://dbpedia.org/property/redirect>"

  private def retainOnlySameAsLinks(predicate: Predicate): Boolean = predicate.equals(OWL_SAMEAS_PREDICATE)

  private def retainOnlyDisambiguatesLinks(predicate: Predicate): Boolean = predicate.equals(DBPEDIA_DISAMBIGUATES_PREDICATE)

  private def retainOnlyRedirectLinks(predicate: Predicate): Boolean = predicate.equals(DBPEDIA_REDIRECT_PREDICATE)

  private val mergedDescriptions = outgoingSameAsAttributes ++ incomingSameAsAttributes ++
    incomingDisambiguatesAttributes ++ outgoingRedirectAttributes

  mergedDescriptions.write(Tsv(args("output")))

}
