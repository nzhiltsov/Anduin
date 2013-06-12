package ru.ksu.niimm.cll.anduin

import com.twitter.scalding.{TextLine, Job, Args}
import util.FixedPathLzoTextLine

/**
 * Given a list of entity URIs with possible duplicates,
 * this processor outputs a list of unique entity URIs
 *
 * @author Nikita Zhiltsov 
 */
class CandidateEntityProcessor(args: Args) extends Job(args) {
  /**
   * reads raw lines
   */
  private val lines = new TextLine(args("input")).read.unique('line)

  lines.groupAll.write(new FixedPathLzoTextLine(args("output")))
}
