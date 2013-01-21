package ru.ksu.niimm.cll.anduin

import com.twitter.scalding._
import util.{FixedPathLzoTextLine, FixedPathLzoTsv}
import com.twitter.scalding.TextLine

/**
 * Given an adjacency list, this processor outputs the sorted list of unique entities
 *
 * @author Nikita Zhiltsov 
 */
class AdjacencyNodeProcessor(args: Args) extends Job(args) {
  private val edges =  TypedTsv[(Int, String, String)](args("input")).read.rename((0, 1, 2) ->('relPredicateId, 'subject, 'object))

  private val subjects = edges.project('subject).unique('subject)

  private val objects = edges.project('object).unique('object).rename('object -> 'subject)

  private val nodes = subjects ++ objects

  nodes.unique('subject).groupAll {
    _.sortBy('subject)
  }.write(new FixedPathLzoTextLine(args("output")))
}
