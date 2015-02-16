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

(def long-sent
  [[0 "they" :PRON :arg 1]
   [1 "seize" :V :root -1]
   [2 "he" :PRON :arg 1]
   [3 "and" :CONJ :cc 1]
   [4 "use" :V :conj 1]
   [5 "violence" :N :arg 4]
   [6 "towards" :CONJ :mod 4]
   [7 "he" :PRON :arg 6]
   [8 "in" :CONJ :mod 11]
   [9 "order" :N :dep 11]
   [10 "to" :TO :aux 11]
   [11 "make" :V :mod 4]
   [12 "he" :PRON :arg 13]
   [13 "sign" :V :arg 11]
   [14 "some" :DET :mod 15]
   [15 "papers" :N :arg 13]
   [16 "to" :TO :aux 17]
   [17 "make" :V :arg 13]
   [18 "over" :CONJ :mod 17]
   [19 "the" :DET :mod 20]
   [20 "girl" :N :mod 22]
   [21 "'s" :POS :mod 20]
   [22 "appreciation" :N :arg 18]
   [23 "of" :CONJ :mod 28]
   [24 "which" :DET :arg 23]
   [25 "he" :PRON :arg 28]
   [26 "may" :MD :aux 28]
   [27 "be" :V :aux 28]
   [28 "trustee" :N :mod 22]
   [29 "to" :TO :mod 28]
   [30 "they" :PRON :arg 29]
   [31 "." :PUNCT -1]])


(def token-indexer (indexer 0))
(def relation-indexer (indexer 1))

(defn sent->graph [sent]
  (let [graph (RGraph. (count sent))
        ids   (.entityIds graph)]
    (doseq [[i word pos rel gov] sent]
      (when gov
        (.addRelation graph i gov (relation-indexer rel))
        (aset ids i (token-indexer [word pos]))))
    graph))

(def graphs (mapv sent->graph sents))

(def big-graph (sent->graph long-sent))
