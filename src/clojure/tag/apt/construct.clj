(ns tag.apt.construct
  (:import (uk.ac.susx.tag.apt APTFactory ArrayAPT APTVisitor RGraph PersistentKVStore Indexer AccumulativeAPTStore)
           (java.io File))
  (:require [tag.apt.util :as util]
            [tag.apt.conll :as conll]
            [tag.apt.canon :as canon]
            [tag.apt.backend :as b]
            [tag.apt.core :refer [count-paths!]]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [tag.apt.backend.leveldb :as leveldb]))

(defn graph->apts [graph]
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
  (let [graph (RGraph. (inc (reduce max 0 (map first sent))))
        ids   (.entityIds graph)]
    (doseq [[i eid govid relid] sent]
      (.addRelation graph i govid relid)
      (aset ids i eid))
    graph))

(defn raw-sentence->indexed-sentence [entity-indexer relation-indexer sentence]
  (mapv (fn [[entity-offset entity governor-offset relation]]
          [(util/to-int entity-offset)
           (.getIndex entity-indexer entity)
           (util/to-int governor-offset)
           (.getIndex relation-indexer relation)])
        sentence))

(defn raw-giga-sentence->graph [entity-indexer relation-indexer sentence]
  (let [graph (RGraph. (count sentence))
        ids (.entityIds graph)]
    (doseq [[i word lemma pos gov-offset dep] sentence]
      (aset ids (Integer. i) (.getIndex entity-indexer (str lemma "/" (canon/canonicalise-pos-tag pos))))
      (when gov-offset
        (.addRelation graph
                      (Integer. i)
                      (Integer. gov-offset)
                      (.getIndex relation-indexer (canon/canonicalise-relation dep)))))
    graph))

(defn sent-extractor [sent-channel file]
  (async/go
    (with-open [in (if (.endsWith (.getName file) ".gz")
                     (util/gz-reader file)
                     (io/reader file))]
      (doseq [sent (conll/parse in)]
        (async/>! sent-channel sent)))))

(defn godochan [chan consume!]
  (async/go
    (loop [x (async/<! chan)]
      (when x (consume! x) (recur (async/<! chan))))))

(defn- get-input-files [dir-or-file]
  (let [f (io/as-file dir-or-file)]
    (if (.isDirectory f)
      (seq (.listFiles f))
      [f])))

(defn -main [input-dir output-dir depth]
  (let [lexicon-descriptor (b/lexicon-descriptor (io/as-file output-dir))
        output-byte-store (leveldb/from-descriptor lexicon-descriptor)
        accumulative-apt-store (AccumulativeAPTStore. output-byte-store (Integer. depth))
        entity-indexer (b/get-entity-index lexicon-descriptor)
        relation-indexer (b/get-relation-index lexicon-descriptor)
        sum (atom (b/get-sum lexicon-descriptor))
        number-of-sentences-processed (atom 0)
        path-counter (b/get-path-counter lexicon-descriptor)
        consume-sentence! (fn [sent]
                            (doseq [[entity-id apt] (->> sent
                                                      (raw-giga-sentence->graph entity-indexer relation-indexer)
                                                      graph->apts)]
                              (.include accumulative-apt-store entity-id apt)
                              (swap! sum inc)
                              (count-paths! path-counter apt depth))
                            (swap! number-of-sentences-processed inc))
        sent-channel (async/chan 1000)
        sent-extractors (map (partial sent-extractor sent-channel)
                            (get-input-files input-dir))
        num-sent-consumers (* 2 (.availableProcessors (Runtime/getRuntime)))
        sent-consumers (take num-sent-consumers (repeatedly #(godochan sent-channel consume-sentence!)))]
    (try

      ; setup reporter
      (add-watch number-of-sentences-processed
                 :reporter
                 (fn [_ _ _ new-val]
                   (when (= 0 (mod new-val 100))
                     (println "done" new-val "sentences"))))
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
