package ru.ksu.niimm.cll.anduin.util

import org.junit.runner.RunWith
import org.specs.Specification
import ru.ksu.niimm.cll.anduin.util.AdjacencyHelper._
import org.specs.runner.{JUnit4, JUnitSuiteRunner}

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class AdjacencyHelperTest extends JUnit4(AdjacencyHelperTestSpec)

object AdjacencyHelperTestSpec extends Specification {
  "Adjacency helper" should {
    "build a hashtable" in {
      def adjacencyList = Iterator(("<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>",
        "<http://www.aktors.org/ontology/portal#Publication>"),
        ("<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>",
          "<http://eprints.rkbexplorer.com/id/caltech/person-1>"),
        ("<http://eprints.rkbexplorer.com/id/caltech/person-2>",
          "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"))
      val entityIdMap: List[String] = entityIdTable(adjacencyList)
      entityIdMap.size must_== 4
      entityIdMap.indexWhere {
        u => u == "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"
      } must_== 1
      entityIdMap.indexWhere {
        u => u == "<http://www.aktors.org/ontology/portal#Publication>"
      } must_== 2
      entityIdMap.indexWhere {
        u => u == "<http://eprints.rkbexplorer.com/id/caltech/person-1>"
      } must_== 0
      entityIdMap.indexWhere {
        u => u == "<http://eprints.rkbexplorer.com/id/caltech/person-2>"
      } must_== 3
    }
    "convert to row-column array" in {
      def adjacencyList = Iterator((
        // 1st edge
        "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>",
        "<http://www.aktors.org/ontology/portal#Publication>"),
        // 2nd edge
        ("<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>",
          "<http://eprints.rkbexplorer.com/id/caltech/person-1>"),
        // 3rd edge
        ("<http://eprints.rkbexplorer.com/id/caltech/person-2>",
          "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>"))
      val entityIdMap = entityIdTable(adjacencyList)
      def arrays = convert(adjacencyList, entityIdMap)
      val rows = arrays.map {
        case (r, c) => r
      }.toList
      val columns = arrays.map {
        case (r, c) => c
      }.toList
      rows.size must_== 3
      columns.size must_== 3
      rows(0) must_== 1
      columns(0) must_== 2
      rows(1) must_== 1
      columns(1) must_== 0
      rows(2) must_== 3
      columns(2) must_== 1
    }
  }
}
