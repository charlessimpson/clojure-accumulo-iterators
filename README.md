This package allows Clojure functions to be used in [Apache Accumulo](http://accumulo.apache.org/) iterators.

Installation
============
Build the package:

    mvn package

Copy the package and dependencies to Accumulo's library:

    cp ~/.m2/repository/org/clojure/clojure/1.3.0/clojure-1.3.0.jar $ACCUMULO_HOME/lib/ext
    cp target/clojure-accumulo-iterators-0.0.1-SNAPSHOT.jar $ACCUMULO_HOME/lib/ext

Usage
=====
The tests in `test/main/java` give an overview of calling the iterators from Java.

For a quick demonstration, from the Accumulo shell:

    createtable foo
    insert "a" "" "b" ""
    insert "a" "" "c" ""
    insert "d" "" "e" ""
    setscaniter -p 20 -class clojure_accumulo.iterators.ClojureFilter -n filter
    false
    (fn [[k v]] (= (str (.getRow k)) "a"))
    scan

Versions
========
This package has been tested against Clojure 1.3.0 and Accumulo 1.4.0.  Because Accumulo tends to break its API between minor releases, this package will probably not work with 1.3.5 or 1.5.0, but it should not be difficult to port to either of those versions.
