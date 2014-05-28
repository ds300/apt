(ns tag.apt.core-test
  (:import (uk.ac.susx.tag.apt Indexer Resolver))
  (:require [clojure.test :refer :all]
            [tag.apt.test-util :refer :all]
            [clojure.string :refer [split split-lines]]
            [tag.apt.core :refer :all]))

(def sents-text
"0 He PRP nsubj 1
1 folded VBP root -1
2 the DT det 5
3 clean JJ amod 5
4 dry JJ amod 5
5 clothes NNS dobj 1

0 We PRP nsubj 1
1 bought VBP root -1
2 some DT det 7
3 slightly JJ advmod 4
4 fizzy JJ amod 7
5 dry JJ amod 7
6 white JJ amod 7
7 wine NN dobj 1

0 I PRP nsubj 1
1 hate VBP root -1
2 sweet JJ amod 3
3 wine NN dobj 1

0 She PRP nsubj 1
1 hung VBD root -1
2 up RP prt 1
3 the DT det 5
4 wet JJ amod 5
5 clothes NNS dobj 1")

(def sents [[
              [0 "He" :PRP :nsubj 1]


             ]])



(def token-index (indexer 0))
(def dep-index (indexer 1))



;(defn add- [line]
;  (let [[_ word pos dep gov] (split line #"\s+")]
;    (Event. (.getIndex token-index (str word "/" pos))
;            (Integer. gov)
;            (.getIndex dep-index dep))))
;
;
;(defn make-sent [text]
;  (let [entries (map #(split % #"\s+"))
;        entities (int-array (map (comp token-index second) entries))])
;  (into-array Event
;              (map make-event
;                   (split-lines text))))
;
;(defn make-sents [text]
;  (mapv make-sent
;      (split text #"\n\n")))
;
;
;(def sents (make-sents sents-text))
;
;(defn sent->pdts [sent]
;  (into [] (PDT/fromSentence sent)))
;
;(defn add-pdt [lexicon [token-index pdt]]
;  (if-let [existing (lexicon token-index)]
;    (assoc lexicon token-index (PDT/merge existing pdt 9999))
;    (assoc lexicon token-index pdt)))
;
;(defn add-sent [lexicon sent]
;  (reduce add-pdt
;          lexicon
;          (map vector (map #(.token %) (filter identity sent))
;               (filter identity (sent->pdts sent)))))
;
;(defn print-pdt [pdt]
;  (.print pdt token-index dep-index))
;
;(try
;  (def lexicon (reduce add-sent {} sents))
;  (catch Exception e (do
;                       (println "jazz man")
;                       (clojure.repl/pst e))))
;
;
;(map print-pdt (vals lexicon))
