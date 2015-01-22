(ns tag.apt.vectors
  (:import [uk.ac.susx.tag.apt APT LRUCachedAPTStore$Builder ArrayAPT Resolver APTVisitor]
           [java.io Writer])
  (:require [tag.apt.backend :as b]
            [tag.apt.backend.leveldb :as leveldb]
            [clojure.java.io :as io]))

(set! *warn-on-reflection* true)

(def REPORT-COUNT 1000)

(defn convert-lexicon [input-lexicon ^Resolver entity-resolver  ^Resolver relation-resolver ^Writer output-writer]
  (let [apts-processed (atom 0)
        last-report-time (atom (System/currentTimeMillis))
        ]
    ; setup reporter
    (add-watch apts-processed
               :reporter
               (fn [_ _ _ new-val]
                 (let [current-time (System/currentTimeMillis)]
                   (when (= 0 (mod new-val REPORT-COUNT))
                     (println "done" new-val "apts." (float (/ REPORT-COUNT (/ (- current-time @last-report-time) 1000))) "apts/s.")
                     (reset! last-report-time current-time)))))

    (doseq [[w ^ArrayAPT w-apt] (seq input-lexicon)]
      (.write output-writer (or ^String (.resolve entity-resolver w) (str w)))
      (.write output-writer "\t")
      (.walk w-apt (reify APTVisitor
                     (visit [_ path apt]
                       (let [path-str (reduce str
                                              (interpose "»" (map #(.resolve relation-resolver %) path)))
                             path-str (str path-str ":")]
                         (doseq [[eid score] (.entityScores apt)]
                           (.write output-writer path-str)
                           (.write output-writer (or ^String (.resolve entity-resolver eid) (str eid)))
                           (.write output-writer "\t")
                           (.write output-writer (Float/toString score))
                           (.write output-writer "\t"))))))
      (.write output-writer "\n")
      (swap! apts-processed inc))))

(defn lru-store [items backend]
  (-> (LRUCachedAPTStore$Builder.)
      (.setMaxDepth Integer/MAX_VALUE)
      (.setFactory ArrayAPT/factory)
      (.setBackend backend)
      (.setMaxItems items)
      (.build)))

(defn -main [input-dir output-file]
  (let [input-descriptor (b/lexicon-descriptor input-dir)
        input-byte-store (leveldb/from-descriptor input-descriptor)
        relation-resolver (b/get-relation-index input-descriptor)
        entity-resolver (b/get-entity-index input-descriptor)]
    (with-open [input-lexicon (lru-store 100000 input-byte-store)
                out ^Writer (io/writer output-file)]
      (convert-lexicon input-lexicon entity-resolver relation-resolver out))))


