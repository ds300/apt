(ns tag.apt.core
  (:import (uk.ac.susx.tag.apt Indexer Resolver)
           (clojure.lang IPersistentMap IDeref)))

(defprotocol RelProto
  (put [this rel]))

(deftype BidirectionalIndex [^IPersistentMap idx2val ^IPersistentMap val2idx]
  RelProto
  (put [this rel]
    (let [c (count idx2val)
          new-idx2val (assoc idx2val c rel)
          new-val2idx (assoc val2idx rel c)]
      (BidirectionalIndex. new-idx2val new-val2idx))))

(defn relation-indexer []
  (let [state (atom (BidirectionalIndex. {} {}))]
    (reify
      Indexer
      (getIndex [this ^String s]
        (if (.startsWith s "_")
          (let [rel (.substring s 1)]
            (- (or (get (.val2idx @state) rel)
                   (dec (count (.val2idx (swap! state put rel)))))))
          (or (get (.val2idx @state) s)
              (dec (count (.val2idx (swap! state put s)))))))

      (hasIndex [this ^String s]
        (boolean (get (.val2idx @state) s)))

      Resolver
      (resolve [this idx]
        (get (.idx2val @state) idx))

      IDeref
      (deref [this] (.val2idx @state)))))

(defn indexer []
  (let [state (atom (BidirectionalIndex. {} {}))]
    (reify
      Indexer
      (getIndex [this ^String s]
        (or (get (.val2idx @state) s)
            (dec (count (.val2idx (swap! state put s))))))

      (hasIndex [this ^String s]
        (boolean (get (.val2idx @state) s)))

      Resolver
      (resolve [this idx]
        (get (.idx2val @state) idx))

      IDeref
      (deref [this] (.val2idx @state)))))
