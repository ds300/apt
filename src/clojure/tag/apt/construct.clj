(ns tag.apt.construct
  (:import (uk.ac.susx.tag.apt APTFactory ArrayAPT APTVisitor RGraph PersistentKVStore Indexer AccumulativeAPTStore))
  (:require [tag.apt.util :as util]
            [clojure.core.async :as async]))

(defprotocol PathCounter
  (count-path! [this path n])
  (get-path-count [this path]))

(defn count-paths! [path-counter apt]
  (.walk apt (reify APTVisitor
               (visit [_ path apt]
                 (count-path! path-counter path (.sum apt))))))


(defn include [^AccumulativeAPTStore store [entity-id apt]]
  (.include store entity-id apt))

(defn graph->apts [graph]
  (let [eids (.entityIds graph)
        len (count eids)
        apts (.fromGraph ArrayAPT/factory)]
    (loop [acc (transient []) i (int 0)]
      (if (< i len)
        (when-let [apt (aget apts i)]
          (conj! acc [(aget eids i) apt]))
        (persistent! acc)))))

(defn indexed-sentence->graph
  "takes a sentence with zero-indexed tokens of the form [entity-index entity-id governor-index relation-id]
  Use -1 as governor-index if the govenor is the root"
  [sent]
  (let [graph (RGraph. (count sent))
        ids   (.entityIds graph)]
    (doseq [[i eid govid relid] sent]
      (.addRelation graph i govid relid)
      (aset ids i eid))
    graph))

(defn raw-sentence->indexed-sentence [entity-indexer relation-indexer sentence]
  (mapv (fn [entity-offset entity governor-offset relation]
          [(util/to-int entity-offset)
           (.getIndex entity-indexer entity)
           (util/to-int governor-offset)
           (.getIndex relation-indexer relation)])
        sentence))


(defn -main [input-dir output-dir])