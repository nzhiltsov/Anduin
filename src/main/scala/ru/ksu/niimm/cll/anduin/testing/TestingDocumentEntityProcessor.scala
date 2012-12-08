package ru.ksu.niimm.cll.anduin.testing

import com.twitter.scalding.Args
import cascading.pipe.Pipe
import ru.ksu.niimm.cll.anduin.DocumentEntityProcessor

/**
 * @author Nikita Zhiltsov 
 */
class TestingDocumentEntityProcessor(args: Args) extends DocumentEntityProcessor(args) {
  override protected def process(pipe: Pipe, offsetName: Symbol) = super.process(pipe, 'num)
}
