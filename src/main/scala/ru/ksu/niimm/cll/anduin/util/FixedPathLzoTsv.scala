package ru.ksu.niimm.cll.anduin.util

import com.twitter.scalding.Tsv
import com.twitter.scalding.commons.source.LzoTsv

/**
 * @author Nikita Zhiltsov 
 */
class FixedPathLzoTsv(path: String) extends Tsv(path) with LzoTsv {
}
