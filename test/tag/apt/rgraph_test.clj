(ns tag.apt.rgraph-test
  (:import (uk.ac.susx.tag.apt RGraph))
  (:require [tag.apt.test.data :as data]
            [clojure.test :refer [deftest is]]))

(deftest topological-sort-test
  (doseq [g data/graphs]
    ; todo: test this jazz with reference implementation mayhaps
    ; best I can do for now is eyeball the results in the REPL
    (let [sorted (.sorted g)
          indices (into #{} sorted)]
      (println (into [] sorted))
      (doseq [n (range (count (.entityIds g)))]
        (is (indices n))))))

(deftest topological-sort-perf
  (time (dorun (pmap #(.sorted ^RGraph %) (repeat 2000000 data/big-graph))))
  )

(topological-sort-perf)