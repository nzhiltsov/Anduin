package ru.ksu.niimm.cll.anduin.util

import scala.Predef._

/**
 * This helper converts an adjacency list to row-column arrays
 *
 * @author Nikita Zhiltsov 
 */
object AdjacencyHelper {

  implicit def pairToList[A](p: (A, A)) = List(p._1, p._2)

  /**
   * returns an array of unique entity URIs
   *
   * @param adjacencyList  a stream of subject-object URI pairs
   * @return
   */
  def entityIdTable(adjacencyList: Iterator[(String, String)]): List[String] =
    adjacencyList.toList.flatten.distinct


  /**
   * converts a stream of subject-object pairs to row-column arrays
   *
   * @param adjacencyList a stream of subject-object URI pairs
   */
  def convert(adjacencyList: Iterator[(String, String)], entityIdTable: List[String]): Iterator[(Int, Int)] = {
    adjacencyList.map {
      entities: (String, String) =>
        (entityIdTable.indexWhere {
          u => u == entities._1
        },
          entityIdTable.indexWhere {
            u => u == entities._2
          })
    }
  }
}
