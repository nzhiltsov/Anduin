package ru.ksu.niimm.cll.anduin.entitysearch

import com.twitter.scalding.{TypedTsv, Job, Args, Tsv}
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTsv

/**
 * This processor implements aggregation of entity description with partial URI resolution according to the paper
 * Neumayer, R., Balog, K. On the Modeling of Entities for Ad-Hoc Entity Search in the Web of Data. ECIR'12
 * <br/>
 * The output format is as follows:
 * <p>
 * <b>predicate_type TAB subject TAB objects</b>
 * </p>
 * where 'predicate_type' values are {0,1,2,3}, i.e., {nameType, attrType, outRelType, inRelType}, correspondingly,
 * and 'objects' may contain more than one literal, the literals are delimited with space, e.g.
 * <p>
 * <b>2	<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>	<http://www.aktors.org/ontology/portal#has-author>	"Tyson" "Johnson"</b>
 * </p>
 *
 * All the unresolved URIs are normalized to simplify further tokenization.
 *
@author Nikita Zhiltsov
 */
class SEMProcessor(args: Args) extends Job(args) {
  val OUTPUT_FILE_NUMBER = 10

  private val nameLikeAttributes =
    TypedTsv[(Int, Subject, Range)](args("inputNameLike")).read.rename((0, 1, 2) ->('predicatetype, 'subject, 'object))

  private val outgoingLinks =
    TypedTsv[(Int, Subject, Range)](args("inputOutgoingLinks")).read.rename((0, 1, 2) ->('predicatetype, 'subject, 'object))

  private val incomingLinks =
    TypedTsv[(Int, Subject, Range)](args("inputIncomingLinks")).read.rename((0, 1, 2) ->('predicatetype, 'subject, 'object))
  /**
   * combines all the pipes into the single final pipe
   */
  private val mergedEntities =
    nameLikeAttributes ++ outgoingLinks ++ incomingLinks


  /**
   * cleans and outputs the data
   */
  mergedEntities
    .groupBy('subject) {
    _.reducers(OUTPUT_FILE_NUMBER).sortBy('subject)
  }
    .write(Tsv(args("output")))


}
