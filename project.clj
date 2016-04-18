(defproject uk.ac.susx.tag/apt "0.8.0-SNAPSHOT"
  :description "Utilities for dealing with Anchored Packed Trees"
  :url "http://susx.ac.uk/"
  :license {:name "Apache License Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [com.google.guava/guava "16.0.1"]
                 [it.unimi.dsi/fastutil "6.5.7"]
                 [org.deeplearning4j/deeplearning4j-nlp "0.0.3.2.5"]
                 [uk.ac.susx.mlcl/Byblo "2.1.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.beust/jcommander "1.48"]
                 [org.fusesource.leveldbjni/leveldbjni-all "1.8"]
                 [pl.edu.icm/JLargeArrays "1.6"]]
  :main uk.ac.susx.tag.apt.tasks.Main
  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]
  :repositories [["oracle" "http://download.oracle.com/maven"]
                 ["sonatype" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                              :snapshots true
                              :releases false
                              :sign-releases false
                              :checksum :fail
                 }]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"])
