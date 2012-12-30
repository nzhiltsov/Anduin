package ru.ksu.niimm.cll.anduin.util

import com.twitter.scalding.{TextLine, Tsv}
import com.twitter.scalding.commons.source.LzoText

/**
 * @author Nikita Zhiltsov 
 */
class FixedPathLzoTextLine(path: String) extends TextLine(path) with LzoText {
}
