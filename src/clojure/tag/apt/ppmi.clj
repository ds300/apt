(ns tag.apt.ppmi
  (:require [tag.apt.util :as util]
            [tag.apt.core :as apt])
  (:import (uk.ac.susx.tag.apt APTFactory APTVisitor ArrayAPT$ScoreMerger2 ArrayAPT$EdgeMergePolicy
                               Int2FloatArraySortedMap))
  (:import (uk.ac.susx.tag.apt BidirectionalIndexer APT ArrayAPT APTFactory)))

(defn from-path
  "delves into an apt strucutre, returning a node at path from apt,
   creating empty nodes if they do not exist"
  [apt [r & more :as path]]
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
(defn freq->pmi [lexicon path-counts apt]
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
          (Int2FloatArraySortedMap. (.keys scores) newvals))))))

(defn convert-lexicon [input-lexicon output-lexicon path-counts]
  (doseq [[w w-apt] (seq input-lexicon)]
    (.put output-lexicon w (freq->pmi input-lexicon path-counts w-apt))))

