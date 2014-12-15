(ns tag.apt.core
  (:import (uk.ac.susx.tag.apt Indexer Resolver BidirectionalIndexer APTVisitor APT)
           (clojure.lang IPersistentMap IDeref IFn)))

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
        (if (.startsWith s "_")
          (let [rel (.substring s 1)]
            (- (or (get (.val2idx @state) rel)
                   (dec (.next (swap! state put! rel))))))
          (or (get (.val2idx @state) s)
              (dec (.next (swap! state put! s))))))

      (hasIndex [this s]
        (boolean (get (.val2idx @state) s)))

      (resolve [this idx]
        (if (< idx 0)
          (when-let [val (get (.idx2val @state) (- idx))]
            (str "_" val))
          (get (.idx2val @state) idx)))

      (getValues [this]
        (keys (.val2idx @state)))

      (getIndices [this]
        (keys (.idx2val @state)))

      IDeref
      (deref [this] (.val2idx @state))

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
        (or (get (.val2idx @state) s)
            (dec (.next (swap! state put! s)))))

      (hasIndex [this s]
        (boolean (get (.val2idx @state) s)))

      (resolve [this idx]
        (get (.idx2val @state) idx))

      (getValues [this]
        (keys (.val2idx @state)))

      (getIndices [this]
        (keys (.idx2val @state)))

      IDeref
      (deref [this] (.val2idx @state))

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

(defn count-paths! [path-counter apt]
  (.walk apt (reify APTVisitor
               (visit [_ path apt]
                 (count-path! path-counter path (.sum apt))))))

(defn path-counter
  ([] (path-counter {}))
  ([init]
   (when-not (map? init) (throw (Exception. "path counter init value should be a map")))
   (let [state (atom init)]
     (reify
       PathCounter
       (count-path! [_ path n]
         (swap! state update-in [(-ensure-vector path)] (fnil + 0) n))
       (get-path-count [_ path]
         (@state (-ensure-vector path)))
       IDeref
       (deref [this]
         @state)))))