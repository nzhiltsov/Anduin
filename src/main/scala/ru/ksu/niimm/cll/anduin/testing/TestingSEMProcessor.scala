package ru.ksu.niimm.cll.anduin.testing

import com.twitter.scalding.Args
import cascading.pipe.Pipe
import ru.ksu.niimm.cll.anduin.SEMProcessor

/**
 * @author Nikita Zhiltsov 
 */
class TestingSEMProcessor(args: Args) extends SEMProcessor(args) {
  override protected def process(pipe: Pipe, offsetName: Symbol) = super.process(pipe, 'num)
}
