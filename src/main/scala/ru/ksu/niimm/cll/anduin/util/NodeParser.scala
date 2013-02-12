package ru.ksu.niimm.cll.anduin.util

import org.apache.commons.lang.StringEscapeUtils
import org.jsoup.Jsoup
import org.jsoup.safety.Whitelist

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


  def extractNodes(line: String): (Context, Subject, Predicate, Range) = {

    val endSubject = line match {
      case l if l.startsWith("_") => line.indexOf(" ") // blank node
      case l if l.startsWith("<") => line.indexOf(">") + 1
      case _ => throw new Exception("can't process such a line")
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

  private val nameLikeAttributes = Array("label", "name", "title")

  /**
   * check if the predicate is 'name'-like, e.g. 'name', 'label', 'title' etc.
   *
   * @param predicate a predicate
   * @return
   */
  def isNamePredicate(predicate: Predicate): Boolean = {
    val pureURI = predicate.toLowerCase
    val elements = if (pureURI.contains('#')) pureURI.split('#') else pureURI.split('/')
    val relativePart = if (elements.length < 2) pureURI
    else elements(elements.length - 1)

    nameLikeAttributes.exists(s => relativePart.contains(s))
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
      .replaceAll(SPECIAL_SYMBOL_PATTERN, " ")
  }
}
