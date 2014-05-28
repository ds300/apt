(ns tag.apt.test.construct.counter
  (:import (it.unimi.dsi.fastutil.ints Int2IntRBTreeCounter Int2IntRBTreeCounter$Entry))
  (:require [clojure.test :refer :all]))


(deftest counter-test
  (let [counter (Int2IntRBTreeCounter.)]
    (.increment counter 0 1)
    (is (= 1 (.get counter 0)))
    (.increment counter 0 4)
    (is (= 5 (.get counter 0)))
    (.increment counter 1 100)
    (is (= 100 (.get counter 1)))
    (.increment counter 5 100)
    (.increment counter 5 100)
    (is (= 200 (.get counter 5)))

    (.increment counter -1 -5)

    (is (= (.size counter) 4))

    (is (= [-1 0 1 5] (into [] (.keySet counter))))
    (is (= [-5 5 100 200] (into [] (.values counter))))))


(defn reference-counter-implementation []
  (let [state (atom {})]
    (fn ([k]
          (@state k))
        ([k v]
          (swap! state update-in [k] (fnil + 0) v)))))


(deftest counter-random-test
  (let [ref (reference-counter-implementation)
        counter (Int2IntRBTreeCounter.)]
    (doseq [i (take 1000000 (repeatedly #(rand-int 50)))]
      (ref i 1)
      (.increment counter i 1))

    (doseq [^Int2IntRBTreeCounter$Entry e (.int2IntEntrySet counter)]
      (is (= (.getIntValue e) (ref (.getIntKey e)))))))

