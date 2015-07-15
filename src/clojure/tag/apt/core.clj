(ns tag.apt.core
  (:import (uk.ac.susx.tag.apt Indexer Resolver BidirectionalIndexer APTVisitor APT)
           (clojure.lang IPersistentMap IDeref IFn Atom)
           [it.unimi.dsi.fastutil.ints Int2ObjectArrayMap]
           [java.util.concurrent.atomic AtomicLong])
  (:require [tag.apt.util :as util]))

(set! *warn-on-reflection* true)

(defn- invert-map [m]
  (persistent! (reduce-kv (fn [a k v] (assoc! a v k))
                          (transient {})
                          m)))

(defprotocol Put
  (put! [this rel]))

(deftype BidirectionalIndex [^long next ^IPersistentMap idx2val ^IPersistentMap val2idx]
  Put
  (put! [this rel]
    (let [new-idx2val (assoc idx2val next rel)
          new-val2idx (assoc val2idx rel next)]
      (BidirectionalIndex. (inc next) new-idx2val new-val2idx))))

(defn- relation-indexer* [^BidirectionalIndex idx]
  (let [state (atom idx)]
    (reify
      BidirectionalIndexer
      (getIndex [this s]
        (if (.startsWith ^String s "_")
          (- (.getIndex this (.substring ^String s 1)))
          (let [idx (get (.val2idx ^BidirectionalIndex @state) s)]
            (or idx
                (locking state
                  (let [idx (get (.val2idx ^BidirectionalIndex @state) s)]
                    (or idx
                        (dec (.next ^BidirectionalIndex (swap! state put! s))))))))))

      (hasIndex [this s]
        (boolean (get (.val2idx ^BidirectionalIndex @state) s)))

      (resolve [this idx]
        (if (< idx 0)
          (when-let [val (get (.idx2val ^BidirectionalIndex @state) (- idx))]
            (str "_" val))
          (get (.idx2val ^BidirectionalIndex @state) idx)))

      (getValues [this]
        (keys (.val2idx ^BidirectionalIndex @state)))

      (getIndices [this]
        (keys (.idx2val ^BidirectionalIndex @state)))

      IDeref
      (deref [this] (.val2idx ^BidirectionalIndex @state))

      IFn
      (invoke [this val]
        (if (or (instance? Long val) (instance? Integer val))
          (.resolve this val)
          (.getIndex this val))))))

(defn relation-indexer
  ([] (relation-indexer* (BidirectionalIndex. 1 {} {})))
  ([val2idx]
    (let [idx2val (invert-map val2idx)]
      (relation-indexer* (BidirectionalIndex. (inc (reduce max 0 (keys idx2val)))
                                              idx2val
                                              val2idx)))))

(defn- indexer* [idx]
  (let [state (atom idx)]
    (reify
      BidirectionalIndexer
      (getIndex [this s]
        (or (get (.val2idx ^BidirectionalIndex @state) s)
            (locking state
              (or (get (.val2idx ^BidirectionalIndex @state) s)
                  (dec (.next ^BidirectionalIndex (swap! state put! s)))))))

      (hasIndex [this s]
        (boolean (get (.val2idx ^BidirectionalIndex @state) s)))

      (resolve [this idx]
        (get (.idx2val ^BidirectionalIndex @state) idx))

      (getValues [this]
        (keys (.val2idx ^BidirectionalIndex @state)))

      (getIndices [this]
        (keys (.idx2val ^BidirectionalIndex @state)))

      IDeref
      (deref [this] (.val2idx ^BidirectionalIndex @state))

      IFn
      (invoke [this val]
        (if (or (instance? Long val) (instance? Integer val))
          (.resolve this val)
          (.getIndex this val))))))



(defn indexer
  ([] (indexer* (BidirectionalIndex. 0 {} {})))
  ([init]
   (indexer*
     (cond
       (number? init)
         (BidirectionalIndex. init {} {})
       (map? init)
         (let [idx2val (invert-map init)]
           (BidirectionalIndex. (inc (reduce max -1 (keys idx2val)))
                                idx2val
                                init))
       :else
         (throw (IllegalArgumentException. (str "Invalid agument type: " (type init))))))))

(defn- -ensure-vector [a-or-v]
  (if (vector? a-or-v)
    a-or-v
    (into [] a-or-v)))

(defprotocol PathCounter
  (count-path! [this path n])
  (get-path-count [this path]))

(defn count-paths!
  ([path-counter apt]
   (count-paths! path-counter apt Integer/MAX_VALUE))
  ([path-counter ^APT apt depth]
    (.walk apt
           (reify APTVisitor
             (visit [_ path apt]
               (count-path! path-counter path (.sum apt))))
           (util/to-int depth))))

(declare count-path!*)
(declare get-path-count*)
(declare to-map)

(deftype PCTuple [^AtomicLong countatom kids]
  PathCounter
  (count-path! [this path n]
    (count-path!* this path (alength ^ints path) (int 0) n))
  (get-path-count [this path]
    (get-path-count* this path (alength ^ints path) 0))
  IDeref
  (deref [this] (to-map this)))

(defn- count-path!* [^PCTuple tup ^ints path ^Integer pathlength ^Integer offset ^Integer n]
  (if (= offset pathlength)
    (.addAndGet ^AtomicLong (.countatom tup) n)
    (let [kid-id (aget path offset)]
      (if-let [^PCTuple kid (get @(.kids tup) kid-id)]
        (recur kid path pathlength (inc offset) n)
        (do (swap! (.kids tup) (fn [state]
                                 (if-let [existing (get state kid-id)]
                                   state
                                   (assoc state kid-id (->PCTuple (AtomicLong. 0) (atom {}))))))
            (recur (get @(.kids tup) kid-id) path pathlength (inc offset) n))))))

(defn- get-path-count* [^PCTuple tup ^ints path ^Integer pathlength ^Integer offset]
  (if (= offset pathlength)
    (.get ^AtomicLong (.countatom tup))
    (if-let [^PCTuple kid (get @(.kids tup) (aget path offset))]
      (recur kid path pathlength (inc offset))
      0)))

(defn- to-map [tup]
  (let [acc (atom (transient {}))
        descend (fn descend [^PCTuple tup path]
                  (let [c (.get ^AtomicLong (.countatom tup))]
                    (when (pos? c)
                      (reset! acc (assoc! @acc path c))))
                  (doseq [[k v] @(.kids tup)]
                    (descend v (conj path k))))]
    (descend tup [])
    (persistent! @acc)))

(defn path-counter2
  ([init] (reduce-kv #(do (count-path! %1 (int-array %2) %3) %1) (path-counter2) init))
  ([]
    (->PCTuple (AtomicLong. 0) (atom {}))))

