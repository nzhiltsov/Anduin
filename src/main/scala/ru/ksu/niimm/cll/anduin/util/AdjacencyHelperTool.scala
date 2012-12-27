package ru.ksu.niimm.cll.anduin.util

import java.io._
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import ru.ksu.niimm.cll.anduin.util.AdjacencyHelper._
import org.slf4j.LoggerFactory
import io.Source

/**
 * This tool reads an adjacency list and, for each predicate, saves row-column representation of adjacency matrices;
 * @see [[ru.ksu.niimm.cll.anduin.AdjacencyListProcessor]] for the input format
 *
 * @author Nikita Zhiltsov 
 */
class AdjacencyHelperTool {
  private val logger = LoggerFactory.getLogger("anduin.AdjacencyHelperTool")

  def main(args: Array[String]) = {
    if (args.length < 2) {
      logger.info("Missing arguments. The Adjacency tool is about to exit.")
      throw new Exception("Missing arguments. The Adjacency tool is about to exit.")
    }
    val inputAdjacencyListFile = args(0)
    val outputDir = args(1)
    new File(outputDir).mkdir

    def table = entityIdTable(readSubjectObjectPairs(inputAdjacencyListFile))

    // saves the results
    readPredicates(inputAdjacencyListFile).foreach {
      predicate =>
        def rowColumnFormat = convert(predicatePairs(inputAdjacencyListFile, predicate), table).toList
        persist(outputDir, predicate, rowColumnFormat)
    }
    printToFile(new File(outputDir, "entity-ids"))(printWriter =>
      table.foreach(printWriter.println))
  }

  def predicatePairs(in: String, predicate: Int) = readTriples(in).filter {
    case (p, s, o) =>
      p == predicate
  }.map {
    case (p, s, o) => (s, o)
  }

  def readPredicates(in: String): List[Int] = readTriples(in).map {
    case (p, s, o) => p
  }.toList.distinct.sorted


  def readSubjectObjectPairs(in: String): Iterator[(String, String)] = readTriples(in).map {
    case (p, s, o) => (s, o)
  }

  def persist(outputDir: String, predicate: Int, rowCols: List[(Int, Int)]) = {
    val rows = rowCols.map {
      case (r, c) => r
    }
    printToFile(new File(outputDir, predicate + "-rows"))(printWriter =>
      rows.foreach(r => printWriter.print(r + " ")))
    val columns = rowCols.map {
      case (r, c) => c
    }
    printToFile(new File(outputDir, predicate + "-cols"))(printWriter =>
      columns.foreach(c => printWriter.print(c + " ")))
  }

  //  def loadLines(in: java.io.BufferedReader): Stream[String] = {
  //    val line = in.readLine
  //    if (line == null) Stream.Empty
  //    else line #:: loadLines(in)
  //  }

  def readTriples(file: String): Iterator[(Int, String, String)] = {
    val in = new BZip2CompressorInputStream(new FileInputStream(file))
    Source.fromInputStream(in).getLines.map {
      line: String =>
        val elems = line.split('\t')
        val predicateType = Integer.parseInt(elems(0))
        val subject = elems(1)
        val range = elems(2)
        (predicateType, subject, range)
    }
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) = {
    val p = new PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }
}
