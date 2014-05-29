(defproject uk.ac.susx.tag/apt "0.1.0-SNAPSHOT"
  :description "Utilities for dealing with Anchored Packed Trees"
  :url "http://susx.ac.uk/"
  :license {:name "Apache License Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.google.guava/guava "16.0.1"]
                 [it.unimi.dsi/fastutil "6.5.7"]
                 [berkeleydb/je "3.2.76"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"])