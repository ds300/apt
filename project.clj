(defproject uk.ac.susx.tag/apt "0.8.0-SNAPSHOT"
  :description "Utilities for dealing with Anchored Packed Trees"
  :url "http://susx.ac.uk/"
  :license {:name "Apache License Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.google.guava/guava "16.0.1"]
                 [it.unimi.dsi/fastutil "6.5.7"]
                 [org.deeplearning4j/deeplearning4j-nlp "0.0.3.2.5"]
                 [uk.ac.susx.mlcl/Byblo "2.1.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.fusesource.leveldbjni/leveldbjni-all "1.8"]]
  :main tag.apt.construct
  :javac-options ["-target" "1.7" "-source" "1.7" "-Xlint:-options"]
  :repositories [["oracle" "http://download.oracle.com/maven"]
                 ["sonatype" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                              :snapshots true
                              :releases false
                              :sign-releases false
                              :checksum :fail
                 }]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"])
