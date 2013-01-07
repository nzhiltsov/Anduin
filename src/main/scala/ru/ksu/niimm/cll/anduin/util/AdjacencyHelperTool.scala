package ru.ksu.niimm.cll.anduin.util

import java.io._
import ru.ksu.niimm.cll.anduin.util.AdjacencyHelper._
import org.slf4j.LoggerFactory
import io.Source
import com.hadoop.compression.lzo.{LzopInputStream, LzopDecompressor}

/**
 * This tool reads an adjacency list and, for each predicate, saves row-column representation of adjacency matrices;
 * @see [[ru.ksu.niimm.cll.anduin.AdjacencyListProcessor]] for the input format
 *
 * @author Nikita Zhiltsov 
 */
object AdjacencyHelperTool {
  private val logger = LoggerFactory.getLogger("anduin.AdjacencyHelperTool")

  def main(args: Array[String]) = {
    if (args.length < 2) {
      logger.info("Missing arguments. The Adjacency tool is about to exit.")
      throw new Exception("Missing arguments. The Adjacency tool is about to exit.")
    }
    val inputAdjacencyListFile = args(0)
    val outputDir = args(1)
    new File(outputDir).mkdir

    val table = entityIdTable(readSubjectObjectPairs(inputAdjacencyListFile))

    // saves the results
    readPredicates foreach {
      predicate =>
        def rowColumnFormat = convert(predicatePairs(inputAdjacencyListFile, predicate), table)
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

  def readPredicates: Iterator[Int] =
    Source.fromInputStream(getClass.getResourceAsStream("/top50-btc-predicates.txt")).getLines.map {
      line: String =>
        val elems = line.split('\t')
        val predicateNum = Integer.parseInt(elems(1))
        predicateNum
    }


  def readSubjectObjectPairs(in: String): Iterator[(String, String)] = readTriples(in).map {
    case (p, s, o) => (s, o)
  }

  def persist(outputDir: String, predicate: Int, rowCols: Iterator[(Int, Int)]) = {
    if (rowCols.hasNext) {
      var edgeNumber = 0
      printToFiles(new File(outputDir, predicate + "-rows"), new File(outputDir, predicate + "-cols"))(
        (rowWriter: PrintWriter, colWriter: PrintWriter) =>
          rowCols.foreach {
            case (r, c) =>
              edgeNumber += 1
              rowWriter.print(r + " ")
              colWriter.print(c + " ")
          })
      logger.info("Predicate with id={} has been done: {} edges.", predicate, edgeNumber)
    }
    else {
      logger.warn("Could find any edges for a predicate with id = {}.", predicate)
    }
  }

  def readTriples(file: String): Iterator[(Int, String, String)] = {
    val lzoBufferSize = 256 * 1024
    val lzoDecompressor = new LzopDecompressor(lzoBufferSize)
    val in = new LzopInputStream(new FileInputStream(file), lzoDecompressor, lzoBufferSize)
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
      p.close
    }
  }

  def printToFiles(first: java.io.File, second: java.io.File)(op: (java.io.PrintWriter, java.io.PrintWriter) => Unit) = {
    val p1 = new PrintWriter(first)
    val p2 = new PrintWriter(second)
    try {
      op(p1, p2)
    } finally {
      p1.close
      p2.close
    }
  }
}
