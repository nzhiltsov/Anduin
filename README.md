Anduin
=================

Processing Large RDF Graphs on Hadoop
------------------------------
Anduin is a lightweight and concise tool to process RDF/N-Quads as well as RDF/NTriples formatted data using Hadoop. Anduin is written in Scala and built atop [Scalding](http://github.com/twitter/scalding), a library from Twitter.

Current Version
------------
[0.2](https://github.com/nzhiltsov/Anduin/archive/0.2.zip)

Features
------------
* Support of [RDF/N-Quads](http://www.w3.org/TR/2014/REC-n-quads-20140225/) and [RDF/NTriples](http://www.w3.org/TR/2014/REC-n-triples-20140225/) formats
* Tolerant to ill-formed RDF data
* Gathering entity type statistics
* Building adjacency matrices
* Aggregating entity descriptions (e.g. for entity search) 

Known Issues
----------------------
There is no support of blank nodes at the moment.


Prerequisites
----------------------
* Java 1.6+
* Scala 2.9.2+
* tested on Apache Hadoop 1.1 as well as Amazon Web Services Elastic MapReduce

Mailing list
------------

Have a question or a suggestion? Please join our [mailing list](https://groups.google.com/d/forum/anduin).

anduin@googlegroups.com

Development and Contribution
----------------------

Anduin has been developed by [Nikita Zhiltsov](http://linkedin.com/in/nzhiltsov). To add new functionality or fix existing bugs, feel free to contribute the patches via pull requests into the _develop_ branch.


License
---------------------

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0




