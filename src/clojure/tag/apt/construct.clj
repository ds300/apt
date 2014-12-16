(ns tag.apt.construct
  (:gen-class)
  (:import (uk.ac.susx.tag.apt APTFactory ArrayAPT APTVisitor RGraph PersistentKVStore Indexer AccumulativeAPTStore)
           (java.io File Reader BufferedReader FileReader))
  (:require [tag.apt.util :as util]
            [tag.apt.conll :as conll]
            [tag.apt.canon :as canon]
            [tag.apt.backend :as b]
            [tag.apt.core :refer [count-paths!]]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [tag.apt.backend.leveldb :as leveldb]))

(set! *warn-on-reflection* true)

(defn graph->apts [^RGraph graph]
  (let [eids (.entityIds graph)
        len (count eids)
        apts (.fromGraph ArrayAPT/factory graph)]
    (loop [acc (transient []) i (int 0)]
      (if (< i len)
        (if-let [apt (aget apts i)]
          (recur (conj! acc [(aget eids i) apt]) (inc i))
          (recur acc (inc i)))
        (persistent! acc)))))

(defn indexed-sentence->graph
  "takes a sentence with [entity-offset entity-id governor-offset relation-id]
  Use -1 as governor-index if the govenor is the root"
  [sent]
  (let [graph (RGraph. (int (inc (reduce max 0 (map first sent)))))
        ids   (.entityIds graph)]
    (doseq [[^int i ^int eid ^int govid ^int relid] sent]
      (.addRelation graph i govid relid)
      (aset ids i eid))
    graph))

(defn raw-sentence->indexed-sentence [^Indexer entity-indexer ^Indexer relation-indexer sentence]
  (mapv (fn [[entity-offset entity governor-offset relation]]
          [(util/to-int entity-offset)
           (.getIndex entity-indexer entity)
           (util/to-int governor-offset)
           (.getIndex relation-indexer relation)])
        sentence))

(defn raw-giga-sentence->graph [^Indexer entity-indexer ^Indexer relation-indexer sentence]
  (let [graph (RGraph. (count sentence))
        ids (.entityIds graph)]
    (doseq [[^String i word lemma pos ^String gov-offset dep] sentence]
      (let [i (Integer. i)]
        (aset ids i (.getIndex entity-indexer (str lemma "/" (canon/canonicalise-pos-tag pos))))
        (when gov-offset
          (.addRelation graph
                        i
                        (Integer. gov-offset)
                        (.getIndex relation-indexer (canon/canonicalise-relation dep))))))
    graph))

(defmacro godochan [[s c] & body]
  `(async/go
    (loop [~s (async/<! ~c)]
       (when (not (nil? ~s))
         ~@body
         (recur (async/<! ~c))))))

(defn sent-extractor [file-channel sent-channel]
  (godochan [file file-channel]
    (with-open [^Reader in (if (.endsWith ^String (.getName ^File file) ".gz")
                             (util/gz-reader file (* 1024 1024))
                             (BufferedReader. (FileReader. file) (* 1024 1024)))]
      (doseq [sent (conll/parse in)]
        (async/>! sent-channel sent)))))



(defn- get-input-files [dir-or-file]
  (let [f (io/as-file dir-or-file)]
    (if (.isDirectory f)
      (seq (.listFiles f))
      [f])))

(def sents-per-report 10000)

(defn -main [input-dir output-dir depth]
  (let [lexicon-descriptor (b/lexicon-descriptor (io/as-file output-dir))
        output-byte-store (leveldb/from-descriptor lexicon-descriptor)
        accumulative-apt-store (AccumulativeAPTStore. output-byte-store (Integer. ^String depth))
        entity-indexer (b/get-entity-index lexicon-descriptor)
        relation-indexer (b/get-relation-index lexicon-descriptor)
        sum (atom (b/get-sum lexicon-descriptor))
        number-of-sentences-processed (atom 0)
        last-report-time (atom (System/currentTimeMillis))
        path-counter (b/get-path-counter lexicon-descriptor)
        consume-sentence! (fn [sent]
                            (doseq [[entity-id apt] (->> sent
                                                      (raw-giga-sentence->graph entity-indexer relation-indexer)
                                                      graph->apts)]
                              (.include accumulative-apt-store entity-id apt)
                              (swap! sum inc)
                              (count-paths! path-counter apt depth)
                              )
                            (swap! number-of-sentences-processed inc))
        file-channel (doto (async/chan)
                       (async/onto-chan (get-input-files input-dir)))
        sent-channel (async/chan 1000)
        sent-extractors (take 5 (repeatedly #(sent-extractor file-channel sent-channel)))
        num-sent-consumers (* 2 (.availableProcessors (Runtime/getRuntime)))
        sent-consumers (take num-sent-consumers (repeatedly #(godochan [sent sent-channel] (consume-sentence! sent))))]
    (try

      ; setup reporter
      (add-watch number-of-sentences-processed
                 :reporter
                 (fn [_ _ _ new-val]
                   (let [current-time (System/currentTimeMillis)]
                     (when (= 0 (mod new-val sents-per-report))
                       (println "done" new-val "sentences." (float (/ sents-per-report (/ (- current-time @last-report-time) 1000))) "sents/s")
                       (reset! last-report-time current-time)))))
      ; start producers
      (dorun sent-extractors)
      ; start consumers
      (dorun sent-consumers)
      ; wait for producers to finish
      (dorun (map async/<!! sent-extractors))
      ; close channel so consumer guys finish
      (async/close! sent-channel)
      ; wait for consumer guys to finish
      (dorun (map async/<!! sent-consumers))

      (finally

        ; clean up resources
        (.close accumulative-apt-store)
        (b/store-entity-index lexicon-descriptor entity-indexer)
        (b/store-relation-index lexicon-descriptor relation-indexer)
        (b/put-sum lexicon-descriptor @sum)
        (b/store-path-counter lexicon-descriptor path-counter)
        ; goodbye world
        (shutdown-agents)))
    ))
