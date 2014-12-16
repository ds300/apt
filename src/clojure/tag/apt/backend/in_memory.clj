(ns tag.apt.backend.in-memory
  (:import (uk.ac.susx.tag.apt PersistentKVStore)
           (clojure.lang IDeref)))

(set! *warn-on-reflection* true)

(defn kv-store
  "uses a simple persistent backing map. Keys must have sensible hashCode/equals semantics obviously.
  values must have sensible equals semantics for CAS to work."
  ([] (kv-store {}))
  ([init]
    (let [store (atom init)]
      (reify
        PersistentKVStore
        (put [_ k v]
          (swap! store assoc k v))
        (get [_ k] (@store k))
        (containsKey [_ k] (contains? @store k))
        (remove [_ k] (swap! store dissoc k))
        (iterator [_] (.iterator (.entrySet @store)))
        (atomicCAS [_ k expected v]
          (swap! store #(if (= expected (% k))
                         (assoc % k v)
                         %)))
        (close [_])
        IDeref
        (deref [_] @store)))))


