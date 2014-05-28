(ns tag.apt.test.data
  (:import (uk.ac.susx.tag.apt RGraph))
  (:require [clojure.string :refer [split]]
            [tag.apt.test.util :refer [indexer]]))

(def sents
  [[[0 "He" :PRP :nsubj 1]
    [1 "folded" :VBP :root -1]
    [2 "the" :DT :det 5]
    [3 "clean" :JJ :amod 5]
    [4 "dry" :JJ :amod 5]
    [5 "clothes" :NNS :dobj 1]]

   [[0 "We" :PRP :nsubj 1]
    [1 "bought" :VBP :root -1]
    [2 "some" :DT :det 7]
    [3 "slightly" :JJ :advmod 4]
    [4 "fizzy" :JJ :amod 7]
    [5 "dry" :JJ :amod 7]
    [6 "white" :JJ :amod 7]
    [7 "wine" :NN :dobj 1]]

   [[0 "I" :PRP :nsubj 1]
    [1 "hate" :VBP :root -1]
    [2 "sweet" :JJ :amod 3]
    [3 "wine" :NN :dobj 1]]

   [[0 "She" :PRP :nsubj 1]
    [1 "hung" :VBD :root -1]
    [2 "up" :RP :prt 1]
    [3 "the" :DT :det 5]
    [4 "wet" :JJ :amod 5]
    [5 "clothes" :NNS :dobj 1]]])


(def token-indexer (indexer 0))
(def relation-indexer (indexer 1))

(defn sent->graph [sent]
  (let [graph (RGraph. (count sent))
        ids   (.entityIds graph)]
    (doseq [[i word pos rel gov] sent]
      (.addRelation graph i gov (relation-indexer rel))
      (aset ids i (token-indexer [word pos])))
    graph))


(def graphs (mapv sent->graph sents))
