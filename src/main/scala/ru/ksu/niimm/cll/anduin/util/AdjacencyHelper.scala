package ru.ksu.niimm.cll.anduin.util

import scala.Predef._
import collection.mutable

/**
 * This helper converts an adjacency list to row-column arrays
 *
 * @author Nikita Zhiltsov 
 */
object AdjacencyHelper {

  implicit def pairToList[A](p: (A, A)) = List(p._1, p._2)

  /**
   * returns a list of unique entity URIs
   *
   * @param adjacencyList  an iterator of subject-object URI pairs
   * @return
   */
  def entityIdTable(adjacencyList: Iterator[(String, String)]): List[String] = {
    val entitySet = new mutable.HashSet[String]()
    adjacencyList.foreach {
      case (entity1, entity2) =>
        entitySet.add(entity1)
        entitySet.add(entity2)
    }
    entitySet.toList
  }


  /**
   * converts an iterator of subject-object pairs to a row-column iterator
   *
   * @param adjacencyList an iterator of subject-object URI pairs
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
