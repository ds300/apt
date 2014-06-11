(ns giga-conll-bdb.core
  (:gen-class)
  (:import [uk.ac.susx.tag.apt AccumulativeAPTStore$Builder RGraph])
  (:require [tag.apt.backend.db :refer [bdb-lexicon]]
            [tag.apt.util :refer [gz-reader pmapall]]
            [tag.apt.conll :refer [parse]]))

(def num-sents (atom 0))
(def last-report (atom (System/currentTimeMillis)))
(def last-n (atom 0))

(defn report! [n]
  (let [now (System/currentTimeMillis)
        time-diff (double (/ (- now @last-report) 1000))
        n-diff (- n @last-n)
        rate (/ n-diff time-diff)]
    (println "done " n " sents. Current rate: " rate "sents/s")
    (reset! last-report now)
    (reset! last-n n)))

(defn done-sent! []
  (let [n (swap! num-sents inc)]
    (when (= 0 (mod n 5000))
      (report! n))))

(defn to-graph [tkn-index dep-index sent]
  (let [graph (RGraph. (count sent))
        ids   (.entityIds graph)]
    (doseq [[idx word lemma pos gov dep] sent]
      (when (and gov dep)
        (let [tkn-id (tkn-index (str word "/" pos))
              dep-id (dep-index dep)
              idx (Integer. idx)
              gov (Integer. gov)]
          (aset ids idx tkn-id)
          (.addRelation graph idx gov dep-id))))
    graph))


(defn process-file! [lexicon file]
  (let [tkn-index (.getEntityIndex lexicon)
        dep-index (.getRelationIndex lexicon)]
    (with-open [in (gz-reader file (.length file))]
      (doseq [sent (parse in)]
        (.include lexicon (to-graph tkn-index dep-index sent))
        (done-sent!)))))


(defn -main
  "I don't do a whole lot ... yet."
  [home dbname depth & input-files]
  (let [store-builder (.setMaxDepth (AccumulativeAPTStore$Builder.) (Integer. depth))]
    (with-open [lexicon (bdb-lexicon home dbname store-builder)]
      (dorun (pmapall (partial process-file! lexicon) (map clojure.java.io/as-file input-files)))
      (shutdown-agents)
      (report! @num-sents))))
