(ns tag.apt.ppmi
  (:require [tag.apt.util :as util]
            [tag.apt.core :as apt]
            [tag.apt.backend :as b]
            [tag.apt.backend.leveldb :as leveldb])
  (:import (uk.ac.susx.tag.apt APTVisitor ArrayAPT$ScoreMerger2
                               Int2FloatArraySortedMap LRUCachedAPTStore ArrayAPT$EdgeMergePolicy LRUCachedAPTStore$Builder APTFactory PersistentKVStore))
  (:import (uk.ac.susx.tag.apt BidirectionalIndexer APT ArrayAPT APTFactory)))

(set! *warn-on-reflection* true)

(defn from-path
  "delves into an apt strucutre, returning a node at path from apt,
   creating empty nodes if they do not exist"
  [^APT apt [r & more :as path]]
  (if r
    (if-let [rapt (.getChild apt r)]
      (from-path rapt more)
      (from-path (.withEdge apt r (.empty ArrayAPT/factory)) path))
    apt))

(defn count-paths [lexicon]
  (let [state (atom {})]
    (doall (util/pmapall-chunked 50
                                 (fn [[_ w-apt]]
                                   (.walk w-apt
                                          (reify APTVisitor
                                            (visit [_ path apt]
                                              (swap! state update-in [(vec path)] (fnil + 0) (.sum apt))))))
                                 (seq lexicon)))
    @state))

(defn reverse-path [path]
  (int-array (reverse (map (partial * -1) path))))


; log (
;     / P(w->p->w') \       ; just the entity count at the node
;    |  -----------  |
;     \ P(w->p->*)  /       ; just the sum of the node
;   -------------------
;     / P(*->p->w') \       ; reverse trick i.e. P(*->p->w') === P(w'->r->*) where r is p reversed.
;    |  -----------  |
;     \ P(*->p->*)  /       ; use path-counts shim
; )
(defn freq->pmi [^PersistentKVStore lexicon path-counts apt]
  ; use the merge facility as a convenient way to traverse an APT while building a new one
  ; TODO: culling scores that fall below some threshold (e.g. 0 in the case of ppmi)
  (ArrayAPT/merge2
    apt
    (.empty ArrayAPT/factory)
    Integer/MAX_VALUE
    (reify ArrayAPT$ScoreMerger2
      (merge [_ apt _ p]
        (let [s (.sum apt)
              count:*->p->* (path-counts (vec p))
              scores (.entityScores apt)
              newvals (float-array (for [[w' n] (seq (.entrySet scores))]
                                     (let [count:w'->r->* (-> (.get lexicon w')
                                                              (.getChildAt (reverse-path p))
                                                              .sum)
                                           numerator (/ n s)
                                           denomintator (/ count:w'->r->* count:*->p->*)]
                                       (Math/log (float (/ numerator denomintator))))))]
          (Int2FloatArraySortedMap. (.keys scores) newvals))))
    ArrayAPT$EdgeMergePolicy/MERGE_WITH_EMPTY))

(defn convert-lexicon [input-lexicon output-lexicon path-counts]
  (let [apts-processed (atom 0)
        last-report-time (atom (System/currentTimeMillis))]
    ; setup reporter
    (add-watch apts-processed
               :reporter
               (fn [_ _ _ new-val]
                 (let [current-time (System/currentTimeMillis)]
                   (when (= 0 (mod new-val 10))
                     (println "done" new-val "apts." (float (/ 10 (/ (- current-time @last-report-time) 1000))) "apts/s.")
                     (reset! last-report-time current-time)))))

    (doseq [[w w-apt] (seq input-lexicon)]
      (.put output-lexicon w (freq->pmi input-lexicon path-counts w-apt))
      (swap! apts-processed inc))))

(defn lru-store [items backend]
  (-> (LRUCachedAPTStore$Builder.)
      (.setMaxDepth Integer/MAX_VALUE)
      (.setFactory ArrayAPT/factory)
      (.setBackend backend)
      (.setMaxItems items)
      (.build)))

(defn -main [input-dir output-dir]
  (let [input-descriptor (b/lexicon-descriptor input-dir)
        input-byte-store (leveldb/from-descriptor input-descriptor)
        output-descriptor (b/lexicon-descriptor output-dir)
        output-byte-store (leveldb/from-descriptor output-descriptor)
        path-counts (b/get-path-counts input-descriptor)]
    (with-open [input-lexicon (lru-store 100000 input-byte-store)
                output-lexicon (lru-store 100 output-byte-store)]
      (convert-lexicon input-lexicon output-lexicon path-counts))))
