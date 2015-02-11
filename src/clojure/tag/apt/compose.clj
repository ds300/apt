(ns tag.apt.compose
  (:require [tag.apt.backend :as b]
            [tag.apt.conll :as conll]
            [tag.apt.construct :as cons]
            [tag.apt.vectors :as v]
            [clojure.java.io :as io]
            [tag.apt.backend.leveldb :as leveldb]
            [tag.apt.canon :as canon])
  (:import (uk.ac.susx.tag.apt LRUCachedAPTStore$Builder ArrayAPT Indexer RGraph)
           (uk.ac.susx.tag.apt JeremyComposer$Builder APTComposer Util APTVisitor Resolver)
           (java.util.zip GZIPOutputStream)
           (java.io ByteArrayOutputStream File)
           (java.util HashMap)))

(set! *warn-on-reflection* true)

(defn parse-file [f]
  (conll/parse (io/reader f)))

(defn sent->graph [^Indexer entity-indexer ^Indexer relation-indexer sentence]
  (let [graph (RGraph. (count sentence))
        ids (.entityIds graph)]
    (doseq [[^String i entity ^String gov-offset dep] sentence]
      (let [i (dec (Integer. i))]
        (aset ids i (.getIndex entity-indexer entity))
        (when (not-empty gov-offset)
          (.addRelation graph
                        i
                        (dec (Integer. gov-offset))
                        (.getIndex relation-indexer (canon/canonicalise-relation dep))))))
    graph))

(defn compress [bytes]
  (let [out (ByteArrayOutputStream.)
        gz-out (GZIPOutputStream. out)]
    (.write gz-out bytes)
    (.flush gz-out)
    (.close gz-out)
    (.flush out)
    (.close out)
    (.toByteArray out)))

(defn encode [apt]
  (Util/base64encode (compress (.toByteArray apt))))

(defn validate [test & strs]
  (when-not test
    (throw (IllegalArgumentException. (apply str (interpose " " strs))))))

(defn minmax [& nums]
  (reduce (fn [[min max :as result] n]
            [(if (< n min) n min) (if (> n max) n max)])
          [Float/MAX_VALUE Float/MIN_VALUE]
          nums))

(defn indexed [seq]
  (map vector (range) seq))

(defn readable-path [^ints path ^Resolver relation-indexer]
  (str (mapv #(.resolve relation-indexer %) path)))

(defn get-idx2path-map [composed root-node]
  (let [acc (HashMap.)
        n (alength composed)]
    (.walk root-node (reify
                       APTVisitor
                       (visit [_ path node]
                         (dotimes [i n]
                           (when (identical? node (aget composed i))
                             (.put acc i path))))))
    acc))

(defn -main [lex-dir alpha-lo alpha-hi intervals & files]
  (let [desc (b/lexicon-descriptor lex-dir)
        [alpha-lo alpha-hi] (minmax (Float. alpha-lo) (Float. alpha-hi))
        intervals (Integer. intervals)
        composer (do
                   (validate (<= 0.0 alpha-lo alpha-hi 1.0) "alpha values must be between 0 and 1 inclusive, you gave" alpha-lo "and" alpha-hi)
                   (validate (pos? intervals) "number of intervals must be >= 1")
                   (-> (JeremyComposer$Builder.)
                       (.alpha alpha-lo alpha-hi intervals)
                       (.topDown)
                       (.build)))
        entity-index (b/get-entity-index desc)
        relation-index (b/get-relation-index desc)
        backend (leveldb/from-descriptor desc)
        lexicon (v/lru-store 100000 backend)]
    (doseq [f (map io/as-file files)]
      (println "processing" (.getName f))
      (let [dir (io/as-file (str (.getName f) "-composed"))
            count (atom 0)]
        (.mkdirs dir)
        (with-open [sents (cons/sent-stream f)]
          (doseq [[idx sent] (indexed sents)]
            (println "sent " (swap! count inc))
            (let [graph (sent->graph entity-index relation-index sent)
                  composed (.compose composer lexicon graph)
                  root-node (aget composed (first (.sorted graph)))
                  idx2path (get-idx2path-map composed root-node)]
              (with-open [out (io/writer (File. dir (str idx ".sent")))]
                (doseq [[t-idx & _ :as row] sent]
                  (let [row (if-let [path (.get idx2path t-idx)]
                              (conj row (str (vec path)) (readable-path path relation-index))
                              row)]
                    (doseq [s (interpose " " row)]
                      (.write out s))
                    (.write out "\n")))
                (.write out "\n")
                (.write out (encode root-node))))))))))