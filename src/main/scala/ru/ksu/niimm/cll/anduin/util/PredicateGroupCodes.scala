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
    * titles
    */
   val TITLES = 1
   /**
    * the rest attributes (datatype property values, including literals)
    */
   val ATTRIBUTES = 2
   /**
    * names from similar entities (w.r.t. dbpedia:redirect, owl:sameAs symmetric transitive properties)
    */
   val SIMILAR_ENTITY_NAMES = 3
   /**
    * category names, w.r.t. 'dcterms:subject' property
    */
   val CATEGORIES = 4
   /**
    * type names, w.r.t. 'rdf:type' property
    */
   val TYPES = 5
   /**
    * names from outgoing links
    */
   val OUTGOING_ENTITY_NAMES = 6
   /**
    * adjacent predicates' names
    */
   val PREDICATE_NAMES = 7
 }
