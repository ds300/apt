(ns tag.apt.test.dsl
  "A simple dsl for quickly defining APTs on the fly for testing"
  (:import (uk.ac.susx.tag.apt ArrayAPT APTFactory)))


(defn apt
  ([]
    (apt {} {}))
  ([scores]
    (apt scores {}))
  ([scores kids]
    (let [with-scores (loop [apt (.empty ArrayAPT/factory) [[k v] & more] (seq scores)]
                        (if k
                          (recur (.withScore apt k v) more)
                          apt))]
      (loop [apt with-scores [[r kid] & more] (seq kids)]
        (if r
          (recur (.withEdge apt r kid) more)
          apt)))))



