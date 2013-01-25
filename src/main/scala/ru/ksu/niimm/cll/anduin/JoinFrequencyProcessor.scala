package ru.ksu.niimm.cll.anduin

import com.twitter.scalding.{Tsv, TypedTsv, Job, Args}

/**
 * Given a list of files in the format:
 * <br/>
 * <b>word&lt;TAB&gt;count</b>
 * <br/>
 * e.g. term&lt;TAB&gt;120
 * <br/>
 * this processor output the joined list of words sorted by their counts from max to min
 *
 * @author Nikita Zhiltsov 
 */
class JoinFrequencyProcessor(args: Args) extends Job(args) {
  private val wordCounts = TypedTsv[(String, Int)](args("input")).read.rename((0, 1) ->('word, 'count))

  wordCounts.groupBy('word) {
    _.sum('count -> 'count)
  }
    .groupAll {
    _.sortBy('count).reverse
  }
    .map('count -> 'count) { // todo: workaround to write integers properly
    c: Int => c
  }
    .write(Tsv(args("output")))
}
