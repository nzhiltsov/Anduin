package ru.ksu.niimm.cll.anduin.util

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
    (context, subject, predicate, range)
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
    (subject, predicate, range)
  }
}
