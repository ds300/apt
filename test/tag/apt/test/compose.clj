(ns tag.apt.test.compose
  (:require [clojure.test :refer [deftest]]
            [tag.apt.compose :refer [-main]]))


(apply -main (clojure.string/split "data 0 1 100 testsents.corebasic.conll.txt" #" "))
