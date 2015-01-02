package ru.ksu.niimm.cll.anduin.util

import org.apache.commons.lang.StringEscapeUtils
import org.jsoup.Jsoup
import org.jsoup.safety.Whitelist
import PredicateGroupCodes._

/**
 * This parser extracts quads and triples from raw lines
 *
 * @author Nikita Zhiltsov 
 */
object NodeParser {
  type Context = String
  type Subject = String
  type Predicate = String
  // "Object", in other words
  type Range = String

  val OWL_SAMEAS_PREDICATE = "<http://www.w3.org/2002/07/owl#sameAs>"
  val DBPEDIA_DISAMBIGUATES_PREDICATE = "<http://dbpedia.org/ontology/wikiPageDisambiguates>"
  val DBPEDIA_REDIRECT_PREDICATE = "<http://dbpedia.org/ontology/wikiPageRedirects>"
  val DBPEDIA_WIKI_PAGE_WIKI_LINK = "<http://dbpedia.org/ontology/wikiPageWikiLink>"
  val DBPEDIA_WIKIPAGE_EXTERNAL_LINK = "<http://dbpedia.org/ontology/wikiPageExternalLink>"

  def extractTripleFromQuad(line: String): (Subject, Predicate, Range) = extractNodes(line) match {
    case (context, s, p, o) => (s, p, o)
  }

  def extractNodes(line: String): (Context, Subject, Predicate, Range) = {

    val endSubject = line match {
      case l if l.startsWith("_") => line.indexOf(" ") // blank node
      case l if l.startsWith("<") => line.indexOf(">") + 1
      case _ => throw new Exception("can't process such a line: " + line)
    }
    val startContext = line.lastIndexOf("<")
    val endContext = line.lastIndexOf(">") + 1
    val startPredicate = line.indexOf("<", endSubject)
    val endPredicate = line.indexOf(">", startPredicate) + 1

    val context = line.substring(startContext, endContext)
    val subject = line.substring(0, endSubject)
    val predicate = line.substring(startPredicate, endPredicate)
    val range = line.substring(endPredicate, startContext).trim
    (context, subject, predicate, range.replace('\t', ' '))
  }

  def extractNodesFromN3(line: String): (Subject, Predicate, Range) = {

    val endSubject = line match {
      case l if l.startsWith("_") => line.indexOf(" ") // blank node
      case l if l.startsWith("<") => line.indexOf(">") + 1
      case _ => throw new Exception("can't process such a line: " + line)
    }
    val startPredicate = line.indexOf("<", endSubject)
    val endPredicate = line.indexOf(">", startPredicate) + 1
    val endingDot = line.lastIndexOf(".") - 1

    val subject = line.substring(0, endSubject)
    val predicate = line.substring(startPredicate, endPredicate)
    val range = line.substring(endPredicate + 1, endingDot)
    (subject, predicate, range.replace('\t', ' ').trim)
  }

  def extractNodesFromNTuple(line: String): (Subject, Predicate, Range) = {
    val endSubject = line match {
      case l if l.startsWith("_") => line.indexOf(" ") // blank node
      case l if l.startsWith("<") => line.indexOf(">") + 1
      case _ => throw new Exception("can't process such a line")
    }
    val startPredicate = line.indexOf("<", endSubject)
    val endPredicate = line.indexOf(">", startPredicate) + 1
    val endObject = line.length

    val subject = line.substring(0, endSubject)
    val predicate = line.substring(startPredicate, endPredicate)
    val range = line.substring(endPredicate + 1, endObject).trim
    (subject, predicate, range.replace('\t', ' '))
  }

  private val nameAttributes = Array("http://dbpedia.org/property/name",
    "http://xmlns.com/foaf/0.1/name",
    "http://xmlns.com/foaf/0.1/givenName",
    "http://xmlns.com/foaf/0.1/surname",
    "http://dbpedia.org/property/officialName",
    "http://dbpedia.org/property/fullname",
    "http://dbpedia.org/property/nativeName",
    "http://dbpedia.org/property/birthName",
    "http://dbpedia.org/property/alternativeNames",
    "http://dbpedia.org/property/nickname",
    "http://dbpedia.org/ontology/birthName",
    "http://dbpedia.org/property/showName",
    "http://dbpedia.org/property/companyName",
    "http://dbpedia.org/property/shipName",
    "http://dbpedia.org/ontology/formerName",
    "http://dbpedia.org/property/clubname",
    "http://dbpedia.org/property/unitName",
    "http://dbpedia.org/property/otherName",
    "http://dbpedia.org/ontology/iupacName",
    "http://dbpedia.org/property/altNames",
    "http://dbpedia.org/property/birthname",
    "http://dbpedia.org/property/names",
    "http://dbpedia.org/property/lakeName")

  private val labelAttributes = Array("http://www.w3.org/2000/01/rdf-schema#label",
    "http://www.w3.org/2004/02/skos/core#prefLabel")

  private val titleAttributes = Array("http://dbpedia.org/ontology/title",
  "http://dbpedia.org/property/shortDescription",
  "http://purl.org/dc/elements/1.1/description",
  "http://dbpedia.org/ontology/office",
  "http://dbpedia.org/property/type")

  private val categoryAttributes = Array("http://purl.org/dc/terms/subject")

  private val typeAttributes = Array("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

  /**
   * check if the predicate is 'name'-like
   *
   * @param predicate a predicate
   * @return
   */
  def isNamePredicate(predicate: Predicate): Boolean = nameAttributes.contains(predicate)

  def isLabelPredicate(predicate: Predicate): Boolean = labelAttributes.contains(predicate)

  def isTitlePredicate(predicate: Predicate): Boolean = titleAttributes.contains(predicate)

  def isCategoryPredicate(predicate: Predicate): Boolean = categoryAttributes.contains(predicate)

  def isTypePredicate(predicate: Predicate): Boolean = typeAttributes.contains(predicate)

  def encodePredicateType(predicate: Predicate, isDatatypeProperty: Boolean): Int = {
    val pureURI = if (predicate.startsWith("<") && predicate.endsWith(">"))
      predicate.substring(1, predicate.length - 1) else predicate
    if (isNamePredicate(pureURI) || isLabelPredicate(pureURI)) NAMES
    else if (isCategoryPredicate(pureURI)) CATEGORIES
    else if (isDatatypeProperty) ATTRIBUTES
    else OUTGOING_ENTITY_NAMES
  }

  def cleanHTMLMarkup: String => String = str => StringEscapeUtils.unescapeHtml(Jsoup.clean(str, Whitelist.none()))

  val URL_ENCODING_ELEMENT_PATTERN: String = "%2[0-9]{1}"
  val HTML_ENCODING_ELEMENT_PATTERN = "(&amp;)"
  val SPECIAL_SYMBOL_PATTERN: String = "[\\._:'/<>!\\)\\(-=\\?]"

  def stripURI(str: String): String = {
    val pureURI = if (str.startsWith("<") && str.endsWith(">")) str.substring(1, str.length - 1) else str
    val elements = if (pureURI.contains('#')) pureURI.split('#') else pureURI.split('/')

    val relativePart = if (elements.length < 2) pureURI
    else elements(elements.length - 1)

    relativePart.
      replaceAll(URL_ENCODING_ELEMENT_PATTERN, " ").replaceAll(HTML_ENCODING_ELEMENT_PATTERN, " ")
      .replaceAll(SPECIAL_SYMBOL_PATTERN, " ").trim
  }
}
