package ru.ksu.niimm.cll.anduin

import com.twitter.scalding._
import util.NodeParser._
import util.{FixedPathLzoTsv, FixedPathLzoTextLine}

/**
 * @author Nikita Zhiltsov
 *
 * @deprecated This functionality should be merged with SEMProcessor
 */
class EntitySortProcessor(args: Args) extends Job(args) {
  /**
   * reads raw lines
   */
  private val lines = new FixedPathLzoTextLine(args("input")).read

  /**
   * extracts the unique quad nodes from lines
   */
  private val quads = lines.mapTo('line ->('predicatetype, 'subject, 'predicate, 'object)) {
    line: String => parseSEMNtuple(line)
  }

  quads
    .groupAll {
    _.sortBy('subject)
  }
    .write(new FixedPathLzoTsv(args("output")))
}
