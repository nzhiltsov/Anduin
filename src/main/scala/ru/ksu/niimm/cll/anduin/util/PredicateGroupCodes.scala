package ru.ksu.niimm.cll.anduin.util

/**
  * @author Nikita Zhiltsov
  */
object PredicateGroupCodes {
  /**
    * names + labels
    */
   val NAMES = 0
   /**
    * the rest attributes (datatype property values, including literals)
    */
   val ATTRIBUTES = 1
   /**
    * names from similar entities (w.r.t. dbpedia:redirect, owl:sameAs symmetric transitive properties)
    */
   val SIMILAR_ENTITY_NAMES = 2
   /**
    * category names, w.r.t. 'dcterms:subject' property
    */
   val CATEGORIES = 3
   /**
    * names from outgoing links
    */
   val OUTGOING_ENTITY_NAMES = 4
 }
