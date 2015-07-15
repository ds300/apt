(ns tag.apt.compose
  (:require [tag.apt.backend :as b]
            [tag.apt.conll :as conll]
            [clojure.tools.cli :as cli]
            [tag.apt.construct :as cons]
            [tag.apt.vectors :as v]
            [clojure.java.io :as io]
            [tag.apt.backend.leveldb :as leveldb]
            [tag.apt.canon :as canon])
  (:import (uk.ac.susx.tag.apt LRUCachedAPTStore$Builder ArrayAPT Indexer RGraph)
           (uk.ac.susx.tag.apt JeremyComposer$Builder APTComposer Util APTVisitor Resolver AdditionComposer OverlayComposer OverlayComposer$SumStarCollpaser)
           (java.util.zip GZIPOutputStream)
           (java.io ByteArrayOutputStream File)
           (java.util HashMap)))

;(set! *warn-on-reflection* true)

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

(defn readable-path
  ([^ints path ^Resolver relation-indexer]
    (apply str (interpose "»" (map #(.resolve relation-indexer %) path))))
  ([^ints path]
    (apply str (interpose "»" path))))

(defn get-idx2path-map [composed root-node]
  (let [acc (HashMap.)
        n (alength composed)]
    (.walk root-node (reify
                       APTVisitor
                       (visit [_ path node]
                         (dotimes [i n]
                           (when (identical? node (aget composed i))
                             (.put acc (int i) path))))))
    acc))

(def cli-options
  [["-c" "--cache-size SIZE" "Max number of APTs in lexicon cache"
    :default 100000
    :parse-fn #(Integer. %)
    :validate [pos? "Must be positive"]]
   ["-s" "--sum" "Use standard sum method"]
   ["-p" "--pmi" "Use standard pmi rather than ppmi (only applies to sum* method)"]
   ["-h" "--help"]])

(defn parse-opts [args]
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
    (if (or (:help options) errors)
      (do
        (println "USAGE: apt compose [options] <lexicon-dir> <files...>")
        (println summary "\n\n" (or errors ""))
        (System/exit 1))
      (let [[lex-dir & files] arguments]
        [lex-dir files options]))))

(defn -main [& args]
  (let [[lex-dir files {:keys [sum cache-size pmi]}] (parse-opts args)
        desc (b/lexicon-descriptor lex-dir)
        composer (if sum
                   (OverlayComposer.)
                   (OverlayComposer/sumStar (b/get-everything-counts desc) (not pmi)))
        entity-index (b/get-entity-index desc)
        relation-index (b/get-relation-index desc)
        backend (leveldb/from-descriptor desc)
        lexicon (v/lru-store cache-size backend)]
    (doseq [f (map io/as-file files)]
      (println "processing" (.getName f))
      (let [dir (io/as-file (str (.getName f) "-composed"))
            count (atom 0)]
        (.mkdirs dir)
        (with-open [in (cons/open-file f)]
          (doseq [[idx sent] (indexed (conll/parse in))]
            (println "sent " (swap! count inc))
            (let [graph (sent->graph entity-index relation-index sent)
                  composed (.compose composer lexicon graph)
                  root-node (aget composed (first (.sorted graph)))
                  idx2path (get-idx2path-map composed root-node)]
              (with-open [out (io/writer (File. dir (str idx ".sent")))]
                (doseq [[t-idx & _ :as row] sent]
                  (let [row (if-let [path (.get idx2path (int (dec (Integer. t-idx))))]
                              (conj row (readable-path path relation-index) (readable-path path))
                              row)]
                    (doseq [s (interpose " " row)]
                      (.write out s))
                    (.write out "\n")))
                (.write out "\n")
                (.write out (encode root-node))))))))))