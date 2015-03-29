package ru.ksu.niimm.cll.anduin.adjacency

import com.twitter.scalding._
import com.twitter.scalding.TextLine
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTextLine

/**
 * Given an adjacency list, this processor outputs the list of unique entities
 *
 * @author Nikita Zhiltsov 
 */
class AdjacencyNodeProcessor(args: Args) extends Job(args) {
  private val edges =  TypedTsv[(Int, String, String)](args("input")).read.rename((0, 1, 2) ->('relPredicateId, 'subject, 'object))

  private val subjects = edges.project('subject).unique('subject)

  private val objects = edges.project('object).unique('object).rename('object -> 'subject)

  private val nodes = subjects ++ objects

  nodes.unique('subject).write(TextLine(args("output")))
}
