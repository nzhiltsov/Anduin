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
  private val maxLineLength = 40000
  /**
   * reads raw lines
   */
  private val lines = new FixedPathLzoTextLine(args("input")).read.filter('line) {
    line: String =>
      line.length < maxLineLength
  }

  /**
   * extracts the unique quad nodes from lines
   */
  private val quads = lines.mapTo('line ->('predicatetype, 'subject, 'object)) {
    line: String => parseSEMNtuple(line)
  }

  quads.unique(('predicatetype, 'subject, 'object))
    .groupBy('subject) {
    _.reducers(10).sortBy('subject)
  }
    .write(new FixedPathLzoTsv(args("output")))
}
