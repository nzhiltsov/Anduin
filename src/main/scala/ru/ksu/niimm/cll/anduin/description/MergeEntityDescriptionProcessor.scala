package ru.ksu.niimm.cll.anduin.description

import com.twitter.scalding.{Job, Args}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import com.twitter.scalding.{Tsv, JobTest, TupleConversions, TypedTsv, TextLine}

/**
 * @author Nikita Zhiltsov 
 */
class MergeEntityDescriptionProcessor(args: Args) extends Job(args) {
  val OUTPUT_FILE_NUMBER = 10

  private val entityAttributes =
    TypedTsv[(Int, Subject, Range)](args("entityAttributes")).read.rename((0, 1, 2) ->('predicatetype, 'subject, 'object))

  private val similarEntityNames =
    TypedTsv[(Int, Subject, Range)](args("similarEntityNames")).read.rename((0, 1, 2) ->('predicatetype, 'subject, 'object))

  private val mergedEntityDescriptions = entityAttributes ++ similarEntityNames

  mergedEntityDescriptions
    .groupBy('subject) {
    _.reducers(OUTPUT_FILE_NUMBER).sortBy(('subject, 'predicatetype))
  }
    .write(Tsv(args("output")))
}
