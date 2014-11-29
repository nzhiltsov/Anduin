package ru.ksu.niimm.cll.anduin.degree

import com.twitter.scalding.{Tsv, TypedTsv, Job, Args}
import ru.ksu.niimm.cll.anduin.util.{FixedPathLzoTsv, FixedPathLzoTextLine}

/**
 * Given a list of triples (predicateId, subjectURI, objectURI),
 * this processor outputs the entityURI node degrees (i.e., the number of adjacent incoming links as well as outgoing links)
 *
 * @author Nikita Zhiltsov 
 */
class NodeDegreeProcessor(args: Args) extends Job(args) {
  private val adjacencyList = TypedTsv[(String, String, String)](args("input")).read.
    rename((0, 1, 2) ->('predicateId, 'subject, 'object))

  private val subjects = adjacencyList.project(('subject))

  private val objects = adjacencyList.project(('object)).rename('object -> 'subject)

  private val nodes = subjects ++ objects

  nodes.groupBy(('subject)) {
    _.size
  }.map('size -> 'size) {
    size: String => Integer.parseInt(size)
  }.write(Tsv(args("output")))
}
